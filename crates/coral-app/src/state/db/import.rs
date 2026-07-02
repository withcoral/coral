use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::ErrorKind;

use super::session::DbRepos;
use super::{CoralDb, DbError, MaterializationRecord};
use crate::bootstrap::AppError;
use crate::sources::catalog::validate_imported_manifest_database_persistence;
use crate::sources::materialization::{
    load_v4_materialization_from_record, materialization_record_from_dir,
};
use crate::sources::model::SourceOrigin;
use crate::state::{AppStateLayout, ConfigStore};
use crate::workspaces::WorkspaceName;
use coral_spec::parse_source_manifest_yaml;

const LEGACY_SOURCE_CATALOG_IMPORT_MARKER: &str = "legacy_source_catalog_imported";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SourceCatalogImportReport {
    pub(crate) workspace_count: usize,
    pub(crate) source_count: usize,
}

pub(crate) async fn import_config_source_catalog(
    db: &CoralDb,
    config_store: &ConfigStore,
    layout: &AppStateLayout,
    now_unix_nanos: i64,
) -> Result<SourceCatalogImportReport, AppError> {
    let _state_lock = config_store.state_lock_exclusive()?;
    let entries = config_store
        .load_config_unlocked()?
        .source_catalog_entries();

    let mut tx = db.begin().await?;
    if tx
        .app_state_markers()
        .contains(LEGACY_SOURCE_CATALOG_IMPORT_MARKER)
        .await?
    {
        tx.commit().await?;
        import_filesystem_source_manifests(db, layout, now_unix_nanos).await?;
        import_filesystem_v4_materializations(db, layout, now_unix_nanos).await?;
        clear_legacy_source_catalog_config(config_store, entries.len());
        return Ok(SourceCatalogImportReport {
            workspace_count: 0,
            source_count: 0,
        });
    }

    let mut workspaces = BTreeSet::new();
    let mut source_count = 0;
    for (workspace_name, source) in &entries {
        if tx
            .sources()
            .get_source(workspace_name, &source.name)
            .await?
            .is_some()
        {
            continue;
        }
        let manifest_yaml = match source.origin {
            SourceOrigin::Bundled => None,
            SourceOrigin::Imported => {
                read_optional_imported_manifest_file(layout, workspace_name, &source.name)?
            }
        };
        if let Some(manifest_yaml) = manifest_yaml.as_deref() {
            validate_imported_manifest_database_persistence(manifest_yaml, &source.variables)?;
        }
        workspaces.insert(workspace_name.clone());
        tx.workspaces()
            .ensure(workspace_name.as_str(), now_unix_nanos)
            .await?;
        tx.sources()
            .upsert_source(workspace_name, source, now_unix_nanos)
            .await?;
        if let Some(manifest_yaml) = manifest_yaml {
            tx.source_manifests()
                .upsert(workspace_name, &source.name, &manifest_yaml, now_unix_nanos)
                .await?;
        }
        let imported = tx
            .sources()
            .get_source(workspace_name, &source.name)
            .await?;
        if imported.as_ref() != Some(source) {
            return Err(AppError::Database(format!(
                "source catalog import failed validation for {workspace_name}:{}",
                source.name
            )));
        }
        source_count += 1;
    }
    if !entries.is_empty() {
        tx.app_state_markers()
            .insert(LEGACY_SOURCE_CATALOG_IMPORT_MARKER, now_unix_nanos)
            .await?;
    }
    tx.commit().await?;

    import_filesystem_source_manifests(db, layout, now_unix_nanos).await?;
    import_filesystem_v4_materializations(db, layout, now_unix_nanos).await?;
    clear_legacy_source_catalog_config(config_store, entries.len());

    Ok(SourceCatalogImportReport {
        workspace_count: workspaces.len(),
        source_count,
    })
}

async fn import_filesystem_source_manifests(
    db: &CoralDb,
    layout: &AppStateLayout,
    now_unix_nanos: i64,
) -> Result<(), AppError> {
    let mut session = db;
    let workspaces = session.workspaces().list().await?;
    for workspace in workspaces {
        let workspace_name = WorkspaceName::parse(&workspace.id)?;
        let sources = session
            .sources()
            .list_workspace_sources(&workspace_name)
            .await?;
        for source in sources
            .into_iter()
            .filter(|source| source.origin == SourceOrigin::Imported)
        {
            if session
                .source_manifests()
                .get(&workspace_name, &source.name)
                .await?
                .is_some()
            {
                continue;
            }

            let Some(manifest_yaml) = read_validated_manifest_for_backfill(
                layout,
                &workspace_name,
                &source.name,
                &source.variables,
            ) else {
                continue;
            };
            let mut tx = db.begin().await?;
            tx.source_manifests()
                .upsert(
                    &workspace_name,
                    &source.name,
                    &manifest_yaml,
                    now_unix_nanos,
                )
                .await?;
            tx.commit().await?;
        }
    }
    Ok(())
}

fn read_validated_manifest_for_backfill(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &crate::sources::SourceName,
    source_variables: &BTreeMap<String, String>,
) -> Option<String> {
    let manifest_yaml = match read_optional_imported_manifest_file(
        layout,
        workspace_name,
        source_name,
    ) {
        Ok(Some(manifest_yaml)) => manifest_yaml,
        Ok(None) => return None,
        Err(error) => {
            tracing::warn!(
                workspace = %workspace_name,
                source = %source_name,
                detail = %error,
                "skipping imported source manifest database backfill because the legacy manifest could not be read"
            );
            return None;
        }
    };

    if let Err(error) =
        validate_imported_manifest_database_persistence(&manifest_yaml, source_variables)
    {
        tracing::warn!(
            workspace = %workspace_name,
            source = %source_name,
            detail = %error,
            "skipping imported source manifest database backfill because the legacy manifest is invalid"
        );
        return None;
    }

    Some(manifest_yaml)
}

async fn import_filesystem_v4_materializations(
    db: &CoralDb,
    layout: &AppStateLayout,
    now_unix_nanos: i64,
) -> Result<(), AppError> {
    let mut session = db;
    for workspace in session.workspaces().list().await? {
        let workspace_name = WorkspaceName::parse(&workspace.id)?;
        for source in session
            .sources()
            .list_workspace_sources(&workspace_name)
            .await?
            .into_iter()
            .filter(|source| source.origin == SourceOrigin::Imported)
        {
            let dir = layout.v4_materialized_dir(&workspace_name, &source.name);
            if !dir.exists()
                || session
                    .materializations()
                    .get(&workspace_name, &source.name)
                    .await?
                    .is_some()
            {
                continue;
            }
            let Some(manifest_yaml) = session
                .source_manifests()
                .get(&workspace_name, &source.name)
                .await?
                .map(|record| record.manifest_yaml)
            else {
                continue;
            };
            let Some(manifest) = v4_backfill_or_skip(
                parse_source_manifest_yaml(&manifest_yaml),
                &workspace_name,
                &source.name,
            ) else {
                continue;
            };
            let Some(v4) = manifest.as_v4() else {
                continue;
            };
            let Some(record) = v4_backfill_or_skip(
                materialization_record_from_dir(&source.name, &dir, now_unix_nanos),
                &workspace_name,
                &source.name,
            ) else {
                continue;
            };
            if v4_backfill_or_skip(
                load_v4_materialization_from_record(&source.name, &manifest_yaml, v4, &record),
                &workspace_name,
                &source.name,
            )
            .is_none()
            {
                continue;
            }
            upsert_imported_v4_materialization(db, &workspace_name, &source.name, &record).await?;
        }
    }
    Ok(())
}

fn v4_backfill_or_skip<T, E: std::fmt::Display>(
    result: Result<T, E>,
    workspace_name: &WorkspaceName,
    source_name: &crate::sources::SourceName,
) -> Option<T> {
    match result {
        Ok(value) => Some(value),
        Err(error) => {
            tracing::warn!(
                workspace = %workspace_name,
                source = %source_name,
                detail = %error,
                "skipping legacy DSL v4 materialization database backfill; re-add the source to regenerate materialized artifacts"
            );
            None
        }
    }
}

async fn upsert_imported_v4_materialization(
    db: &CoralDb,
    workspace_name: &WorkspaceName,
    source_name: &crate::sources::SourceName,
    record: &MaterializationRecord,
) -> Result<(), AppError> {
    let mut tx = db.begin().await?;
    match tx
        .materializations()
        .upsert(workspace_name, source_name, record)
        .await
    {
        Ok(()) => tx.commit().await.map_err(AppError::from),
        Err(error) if is_unique_constraint_error(&error) => {
            tx.rollback().await?;
            let mut session = db;
            if session
                .materializations()
                .get(workspace_name, source_name)
                .await?
                .is_some()
            {
                Ok(())
            } else {
                Err(error.into())
            }
        }
        Err(error) => Err(error.into()),
    }
}

fn is_unique_constraint_error(error: &DbError) -> bool {
    matches!(error, DbError::Sqlx(sqlx::Error::Database(database_error)) if database_error.is_unique_violation())
}

fn read_imported_manifest_file(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &crate::sources::SourceName,
) -> Result<String, AppError> {
    let manifest_path = layout.manifest_file(workspace_name, source_name);
    fs::read_to_string(&manifest_path).map_err(|error| {
        if error.kind() == ErrorKind::NotFound {
            AppError::SourceNotFound(format!(
                "manifest for imported source '{workspace_name}:{source_name}' at {}",
                manifest_path.display()
            ))
        } else {
            AppError::Io(error)
        }
    })
}

fn read_optional_imported_manifest_file(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &crate::sources::SourceName,
) -> Result<Option<String>, AppError> {
    match read_imported_manifest_file(layout, workspace_name, source_name) {
        Ok(manifest_yaml) => Ok(Some(manifest_yaml)),
        Err(AppError::SourceNotFound(message)) => {
            tracing::warn!(
                detail = %message,
                "imported source manifest file is missing; source metadata will remain without a database manifest row"
            );
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn clear_legacy_source_catalog_config(config_store: &ConfigStore, source_count: usize) {
    if source_count != 0
        && let Err(error) = config_store.clear_source_catalog_unlocked()
    {
        tracing::warn!(
            detail = %error,
            "source catalog imported into database but legacy config cleanup failed"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;

    use tempfile::tempdir;

    use super::{SourceCatalogImportReport, import_config_source_catalog};
    use crate::credentials::CredentialStorageKind;
    use crate::sources::SourceName;
    use crate::sources::materialization::{
        MaterializationInputs, build_v4_materialization_tmp, replace_v4_materialization,
    };
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::state::db::session::DbRepos;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig};
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::workspaces::WorkspaceName;
    use coral_spec::parse_source_manifest_yaml;

    const OPENAPI_FIXTURE: &str = r#"{"openapi":"3.0.3","servers":[{"url":"https://api.example.com"}],"paths":{"/issues":{"get":{"operationId":"issues/list","responses":{"200":{"content":{"application/json":{"schema":{"type":"array","items":{"type":"object","properties":{"id":{"type":"integer"},"title":{"type":"string"}}}}}}}}}}}}"#;

    #[tokio::test]
    async fn imports_config_source_catalog_into_database() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = source(
            "github",
            Some("1.2.3"),
            [("GITHUB_API_BASE", "https://api.github.com")],
            ["GITHUB_TOKEN"],
            Some(CredentialStorageKind::Keychain),
            SourceOrigin::Imported,
        );
        config_store
            .upsert_source(&workspace, source.clone())
            .expect("write config source");
        let manifest_yaml = imported_manifest_yaml("github", "1.2.3");
        write_manifest_file(&layout, &workspace, &source.name, &manifest_yaml);
        let db = open_sqlite(&layout).await;

        let report = import_config_source_catalog(&db, &config_store, &layout, 11)
            .await
            .expect("import source catalog");

        assert_eq!(
            report,
            SourceCatalogImportReport {
                workspace_count: 1,
                source_count: 1,
            }
        );
        let mut session = &db;
        assert!(
            session
                .workspaces()
                .get(workspace.as_str())
                .await
                .expect("get workspace")
                .is_some()
        );
        assert_eq!(
            session
                .sources()
                .get_source(&workspace, &source.name)
                .await
                .expect("get source"),
            Some(source.clone())
        );
        assert_eq!(
            session
                .source_manifests()
                .get(&workspace, &source.name)
                .await
                .expect("get source manifest")
                .expect("source manifest")
                .manifest_yaml,
            manifest_yaml
        );
        assert!(
            layout.manifest_file(&workspace, &source.name).exists(),
            "legacy manifest file should be preserved for rollback compatibility"
        );
    }

    #[tokio::test]
    async fn invalid_imported_config_manifest_rolls_back_catalog_import() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = unsafe_secret_endpoint_source();
        config_store
            .upsert_source(&workspace, source.clone())
            .expect("write config source");
        let manifest_yaml = unsafe_secret_endpoint_manifest_yaml("github", "1.2.3");
        write_manifest_file(&layout, &workspace, &source.name, &manifest_yaml);
        let db = open_sqlite(&layout).await;

        let error = import_config_source_catalog(&db, &config_store, &layout, 11)
            .await
            .expect_err("unsafe legacy manifest should fail active config import");
        let crate::bootstrap::AppError::InvalidInput(message) = error else {
            panic!("expected invalid input error, got {error:?}");
        };
        assert!(message.contains("base_url must use https"));
        let mut session = &db;
        assert!(
            session
                .sources()
                .get_source(&workspace, &source.name)
                .await
                .expect("get source after rollback")
                .is_none()
        );
        assert!(
            !session
                .app_state_markers()
                .contains(super::LEGACY_SOURCE_CATALOG_IMPORT_MARKER)
                .await
                .expect("legacy marker should not be inserted")
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_legacy_config_cleanup_does_not_reimport_stale_sources() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let db_path = temp.path().join("db").join("coral.db");
        fs::create_dir_all(db_path.parent().expect("db parent")).expect("create db dir");
        fs::write(
            layout.config_file(),
            format!(
                "[database]\nbackend = \"sqlite\"\npath = \"{}\"\n",
                db_path.display()
            ),
        )
        .expect("write database config");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = source("github", None, [], [], None, SourceOrigin::Bundled);
        config_store
            .upsert_source(&workspace, source.clone())
            .expect("write config source");
        drop(
            config_store
                .state_lock_exclusive()
                .expect("create state lock before read-only config dir"),
        );
        let db = open_sqlite(&layout).await;
        fs::set_permissions(layout.config_dir(), fs::Permissions::from_mode(0o500))
            .expect("make config dir read-only");
        let report = import_config_source_catalog(&db, &config_store, &layout, 11).await;
        fs::set_permissions(layout.config_dir(), fs::Permissions::from_mode(0o700))
            .expect("restore config dir permissions");
        assert_eq!(
            report.expect("cleanup failure should not fail committed import"),
            SourceCatalogImportReport {
                workspace_count: 1,
                source_count: 1,
            }
        );

        let mut stale_config_source = source.clone();
        stale_config_source
            .variables
            .insert("OWNER".to_string(), "coral".to_string());
        config_store
            .upsert_source(&workspace, stale_config_source)
            .expect("update config source");
        {
            let mut tx = db.begin().await.expect("begin delete tx");
            tx.sources()
                .remove_source(&workspace, &source.name)
                .await
                .expect("delete db source");
            tx.commit().await.expect("commit delete tx");
        }

        let report = import_config_source_catalog(&db, &config_store, &layout, 99)
            .await
            .expect("stale config should not reimport after marker");

        assert_eq!(
            report,
            SourceCatalogImportReport {
                workspace_count: 0,
                source_count: 0,
            }
        );
        let mut session = &db;
        assert_eq!(
            session
                .sources()
                .get_source(&workspace, &source.name)
                .await
                .expect("source should remain deleted"),
            None
        );
        assert!(
            config_store
                .load_config_unlocked()
                .expect("load cleaned stale config")
                .source_catalog_entries()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn empty_config_import_preserves_existing_database_sources() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = source("github", None, [], [], None, SourceOrigin::Bundled);
        let db = open_sqlite(&layout).await;
        {
            let mut tx = db.begin().await.expect("begin tx");
            tx.workspaces()
                .ensure(workspace.as_str(), 11)
                .await
                .expect("ensure workspace");
            tx.sources()
                .upsert_source(&workspace, &source, 11)
                .await
                .expect("seed db source");
            tx.commit().await.expect("commit tx");
        }
        let report = import_config_source_catalog(&db, &config_store, &layout, 22)
            .await
            .expect("import empty source catalog");

        assert_eq!(
            report,
            SourceCatalogImportReport {
                workspace_count: 0,
                source_count: 0,
            }
        );
        let mut session = &db;
        assert_eq!(
            session
                .sources()
                .get_source(&workspace, &source.name)
                .await
                .expect("get preserved source"),
            Some(source)
        );
    }

    #[tokio::test]
    async fn partial_database_catalog_imports_missing_config_sources_without_overwriting_existing()
    {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let config_workspace = WorkspaceName::parse("other").expect("workspace");
        let db_source = source("github", None, [], [], None, SourceOrigin::Bundled);
        let mut stale_config_source = db_source.clone();
        stale_config_source.version = Some("9.9.9".to_string());
        stale_config_source
            .variables
            .insert("OWNER".to_string(), "stale".to_string());
        stale_config_source.origin = SourceOrigin::Imported;
        let config_source = source("slack", None, [], [], None, SourceOrigin::Bundled);
        config_store
            .upsert_source(&workspace, stale_config_source)
            .expect("write stale config source");
        config_store
            .create_workspace(&config_workspace)
            .expect("create config workspace");
        config_store
            .upsert_source(&config_workspace, config_source.clone())
            .expect("write config source");
        let db = open_sqlite(&layout).await;
        {
            let mut tx = db.begin().await.expect("begin tx");
            tx.workspaces()
                .ensure(workspace.as_str(), 11)
                .await
                .expect("ensure workspace");
            tx.sources()
                .upsert_source(&workspace, &db_source, 11)
                .await
                .expect("seed db source");
            tx.commit().await.expect("commit tx");
        }

        let report = import_config_source_catalog(&db, &config_store, &layout, 22)
            .await
            .expect("import source catalog");

        assert_eq!(
            report,
            SourceCatalogImportReport {
                workspace_count: 1,
                source_count: 1,
            }
        );
        let mut session = &db;
        assert_eq!(
            session
                .sources()
                .get_source(&workspace, &db_source.name)
                .await
                .expect("get existing db source"),
            Some(db_source)
        );
        assert_eq!(
            session
                .sources()
                .get_source(&config_workspace, &config_source.name)
                .await
                .expect("get imported config source"),
            Some(config_source)
        );
        assert!(
            config_store
                .load_config_unlocked()
                .expect("load cleaned config")
                .source_catalog_entries()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn empty_config_catalog_import_is_noop() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let db = open_sqlite(&layout).await;

        let report = import_config_source_catalog(&db, &config_store, &layout, 11)
            .await
            .expect("import empty catalog");

        assert_eq!(
            report,
            SourceCatalogImportReport {
                workspace_count: 0,
                source_count: 0,
            }
        );
        let mut session = &db;
        assert!(
            session
                .workspaces()
                .list()
                .await
                .expect("list workspaces")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn empty_config_catalog_does_not_complete_legacy_import() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = source("github", None, [], [], None, SourceOrigin::Bundled);
        let db = open_sqlite(&layout).await;

        let empty_report = import_config_source_catalog(&db, &config_store, &layout, 11)
            .await
            .expect("import empty catalog");
        config_store
            .upsert_source(&workspace, source.clone())
            .expect("write config source after empty import");
        let source_report = import_config_source_catalog(&db, &config_store, &layout, 99)
            .await
            .expect("import source catalog after empty import");

        assert_eq!(
            empty_report,
            SourceCatalogImportReport {
                workspace_count: 0,
                source_count: 0,
            }
        );
        assert_eq!(
            source_report,
            SourceCatalogImportReport {
                workspace_count: 1,
                source_count: 1,
            }
        );
        let mut session = &db;
        assert_eq!(
            session
                .sources()
                .get_source(&workspace, &source.name)
                .await
                .expect("get source"),
            Some(source)
        );
    }

    #[tokio::test]
    async fn imported_config_source_without_manifest_file_keeps_source_without_manifest_row() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = source("github", None, [], [], None, SourceOrigin::Imported);
        config_store
            .upsert_source(&workspace, source.clone())
            .expect("write config source");
        let db = open_sqlite(&layout).await;

        let report = import_config_source_catalog(&db, &config_store, &layout, 11)
            .await
            .expect("missing imported manifest should not block source catalog import");

        assert_eq!(
            report,
            SourceCatalogImportReport {
                workspace_count: 1,
                source_count: 1,
            }
        );
        let mut session = &db;
        assert_eq!(
            session
                .sources()
                .get_source(&workspace, &source.name)
                .await
                .expect("get source"),
            Some(source.clone())
        );
        assert_eq!(
            session
                .source_manifests()
                .get(&workspace, &source.name)
                .await
                .expect("get missing source manifest"),
            None
        );
        assert_eq!(
            config_store
                .load_config_unlocked()
                .expect("legacy config should be cleaned after source catalog import")
                .source_catalog_entries(),
            Vec::new()
        );

        let second_report = import_config_source_catalog(&db, &config_store, &layout, 22)
            .await
            .expect("missing imported manifest should not block later backfill attempts");
        assert_eq!(
            second_report,
            SourceCatalogImportReport {
                workspace_count: 0,
                source_count: 0,
            }
        );
        assert_eq!(
            session
                .source_manifests()
                .get(&workspace, &source.name)
                .await
                .expect("get still-missing source manifest"),
            None
        );
    }

    #[tokio::test]
    async fn invalid_filesystem_manifest_backfill_skips_source_manifest() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let unsafe_source = unsafe_secret_endpoint_source();
        let manifest_yaml = unsafe_secret_endpoint_manifest_yaml("github", "1.2.3");
        write_manifest_file(&layout, &workspace, &unsafe_source.name, &manifest_yaml);
        let healthy = source("healthy_v4", None, [], [], None, SourceOrigin::Imported);
        let corrupt = source("corrupt_v4", None, [], [], None, SourceOrigin::Imported);
        let healthy_descriptor = temp.path().join("healthy-openapi.json");
        fs::write(&healthy_descriptor, OPENAPI_FIXTURE).expect("write OpenAPI fixture");
        let healthy_manifest = format!(
            "name: {}\ndsl_version: 4\nsurfaces:\n- id: rest\n  type: openapi\n  file: {}\n",
            healthy.name,
            healthy_descriptor.display()
        );
        let parsed_manifest = parse_source_manifest_yaml(&healthy_manifest)
            .expect("parse manifest")
            .as_v4()
            .expect("v4 manifest")
            .clone();
        let build = build_v4_materialization_tmp(
            &layout,
            &workspace,
            &healthy.name,
            &healthy_manifest,
            &parsed_manifest,
            &MaterializationInputs::default(),
            "test",
        );
        replace_v4_materialization(
            &layout,
            &workspace,
            &healthy.name,
            &build.expect("build materialization").temp_dir,
        )
        .expect("install legacy materialization");
        let corrupt_manifest = healthy_manifest.replace("healthy_v4", "corrupt_v4");
        fs::create_dir_all(layout.v4_materialized_dir(&workspace, &corrupt.name))
            .expect("create corrupt materialization dir");
        fs::write(
            layout.v4_fingerprint_file(&workspace, &corrupt.name),
            "not: [yaml",
        )
        .expect("corrupt fingerprint");
        let db = open_sqlite(&layout).await;
        let mut tx = db.begin().await.expect("begin tx");
        tx.workspaces()
            .ensure(workspace.as_str(), 7)
            .await
            .expect("ensure workspace");
        tx.sources()
            .upsert_source(&workspace, &unsafe_source, 7)
            .await
            .expect("write source without manifest row");
        for (source, manifest) in [(&healthy, &healthy_manifest), (&corrupt, &corrupt_manifest)] {
            tx.sources()
                .upsert_source(&workspace, source, 7)
                .await
                .expect("upsert source");
            tx.source_manifests()
                .upsert(&workspace, &source.name, manifest, 7)
                .await
                .expect("upsert manifest");
        }
        tx.commit().await.expect("commit sources");

        import_config_source_catalog(&db, &config_store, &layout, 11)
            .await
            .expect("invalid backfills should not fail startup import");

        let mut session = &db;
        assert!(
            session
                .source_manifests()
                .get(&workspace, &unsafe_source.name)
                .await
                .expect("get skipped source manifest")
                .is_none()
        );
        let mut materializations = session.materializations();
        assert!(
            materializations
                .get(&workspace, &healthy.name)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            materializations
                .get(&workspace, &corrupt.name)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn import_succeeds_when_post_commit_config_cleanup_fails() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = source("github", None, [], [], None, SourceOrigin::Bundled);
        config_store
            .upsert_source(&workspace, source.clone())
            .expect("write config source");
        drop(
            config_store
                .state_lock_exclusive()
                .expect("create state lock before making config dir read-only"),
        );

        let db_path = temp.path().join("db").join("coral.db");
        fs::create_dir_all(db_path.parent().expect("db parent")).expect("create db dir");
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path: db_path })
            .await
            .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");

        let original_mode = fs::metadata(layout.config_dir())
            .expect("config dir metadata")
            .permissions()
            .mode();
        fs::set_permissions(layout.config_dir(), fs::Permissions::from_mode(0o500))
            .expect("make config dir read-only");

        let result = import_config_source_catalog(&db, &config_store, &layout, 11).await;

        fs::set_permissions(
            layout.config_dir(),
            fs::Permissions::from_mode(original_mode),
        )
        .expect("restore config dir permissions");

        assert_eq!(
            result.expect("cleanup failure should not fail committed import"),
            SourceCatalogImportReport {
                workspace_count: 1,
                source_count: 1,
            }
        );
        let mut session = &db;
        assert_eq!(
            session
                .sources()
                .get_source(&workspace, &source.name)
                .await
                .expect("get imported source"),
            Some(source.clone())
        );
        assert_eq!(
            config_store
                .load_config_unlocked()
                .expect("legacy config should still load after failed cleanup")
                .source_catalog_entries()
                .len(),
            1
        );

        let mut tx = db.begin().await.expect("begin delete tx");
        tx.sources()
            .remove_source(&workspace, &source.name)
            .await
            .expect("delete db source");
        tx.commit().await.expect("commit delete tx");

        let report = import_config_source_catalog(&db, &config_store, &layout, 99)
            .await
            .expect("stale config should not reimport after marker");

        assert_eq!(
            report,
            SourceCatalogImportReport {
                workspace_count: 0,
                source_count: 0,
            }
        );
        let mut session = &db;
        assert_eq!(
            session
                .sources()
                .get_source(&workspace, &source.name)
                .await
                .expect("source should remain deleted"),
            None
        );
        assert!(
            config_store
                .load_config_unlocked()
                .expect("load cleaned stale config")
                .source_catalog_entries()
                .is_empty()
        );
    }

    async fn open_sqlite(layout: &AppStateLayout) -> CoralDb {
        let config = DatabaseConfig::load(layout).expect("db config");
        let DatabaseConfig::Sqlite { path } = config else {
            panic!("default test config should be sqlite");
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");
        db
    }

    fn source<const V: usize, const S: usize>(
        name: &str,
        version: Option<&str>,
        variables: [(&str, &str); V],
        secrets: [&str; S],
        credential_storage: Option<CredentialStorageKind>,
        origin: SourceOrigin,
    ) -> InstalledSource {
        InstalledSource {
            name: SourceName::parse(name).expect("source name"),
            version: version.map(str::to_string),
            variables: variables
                .into_iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect::<BTreeMap<_, _>>(),
            secrets: secrets.into_iter().map(str::to_string).collect(),
            credential_storage,
            origin,
        }
    }

    fn write_manifest_file(
        layout: &AppStateLayout,
        workspace: &WorkspaceName,
        source_name: &SourceName,
        manifest_yaml: &str,
    ) {
        let manifest_path = layout.manifest_file(workspace, source_name);
        fs::create_dir_all(manifest_path.parent().expect("manifest parent"))
            .expect("create manifest parent");
        fs::write(manifest_path, manifest_yaml).expect("write manifest file");
    }

    fn imported_manifest_yaml(name: &str, version: &str) -> String {
        format!(
            r"
name: {name}
version: {version}
dsl_version: 3
backend: http
base_url: https://example.com
tables:
  - name: messages
    description: Demo messages
    request:
      method: GET
      path: /messages
    response: {{}}
    columns:
      - name: id
        type: Utf8
"
        )
    }

    fn unsafe_secret_endpoint_source() -> InstalledSource {
        source(
            "github",
            Some("1.2.3"),
            [("API_BASE", "http://api.example.com")],
            ["API_TOKEN"],
            Some(CredentialStorageKind::Keychain),
            SourceOrigin::Imported,
        )
    }

    fn unsafe_secret_endpoint_manifest_yaml(name: &str, version: &str) -> String {
        imported_manifest_yaml(name, version).replacen(
            "base_url: https://example.com",
            r#"base_url: "{{input.API_BASE}}"
inputs: { API_BASE: { kind: variable }, API_TOKEN: { kind: secret } }
auth: { type: HeaderAuth, headers: [{ name: Authorization, from: template, template: "Bearer {{input.API_TOKEN}}" }] }"#,
            1,
        )
    }
}
