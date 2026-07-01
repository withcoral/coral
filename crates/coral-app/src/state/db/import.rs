use std::collections::BTreeSet;

use super::CoralDb;
use super::session::DbRepos;
use crate::bootstrap::AppError;
use crate::state::ConfigStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SourceCatalogImportReport {
    pub(crate) workspace_count: usize,
    pub(crate) source_count: usize,
}

pub(crate) async fn import_config_source_catalog(
    db: &CoralDb,
    config_store: &ConfigStore,
    now_unix_nanos: i64,
) -> Result<SourceCatalogImportReport, AppError> {
    let _state_lock = config_store.state_lock_exclusive()?;
    let entries = config_store
        .load_config_unlocked()?
        .source_catalog_entries();

    let mut tx = db.begin().await?;
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
        workspaces.insert(workspace_name.clone());
        tx.workspaces()
            .ensure(workspace_name.as_str(), now_unix_nanos)
            .await?;
        tx.sources()
            .upsert_source(workspace_name, source, now_unix_nanos)
            .await?;
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
    tx.commit().await?;

    Ok(SourceCatalogImportReport {
        workspace_count: workspaces.len(),
        source_count,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tempfile::tempdir;

    use super::{SourceCatalogImportReport, import_config_source_catalog};
    use crate::credentials::CredentialStorageKind;
    use crate::sources::SourceName;
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::state::db::session::DbRepos;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig};
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::workspaces::WorkspaceName;

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
        let db = open_sqlite(&layout).await;

        let report = import_config_source_catalog(&db, &config_store, 11)
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
            Some(source)
        );
    }

    #[tokio::test]
    async fn reimport_preserves_existing_database_catalog() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let mut source = source("github", None, [], [], None, SourceOrigin::Bundled);
        config_store
            .upsert_source(&workspace, source.clone())
            .expect("write config source");
        let db = open_sqlite(&layout).await;
        import_config_source_catalog(&db, &config_store, 11)
            .await
            .expect("initial import");

        let original_source = source.clone();
        source
            .variables
            .insert("OWNER".to_string(), "coral".to_string());
        config_store
            .upsert_source(&workspace, source.clone())
            .expect("update config source");
        let report = import_config_source_catalog(&db, &config_store, 99)
            .await
            .expect("reimport source catalog");

        assert_eq!(
            report,
            SourceCatalogImportReport {
                workspace_count: 0,
                source_count: 0,
            }
        );
        let mut session = &db;
        let workspace_record = session
            .workspaces()
            .get(workspace.as_str())
            .await
            .expect("get workspace")
            .expect("workspace row");
        assert_eq!(workspace_record.created_at_unix_nanos, 11);
        assert_eq!(
            session
                .sources()
                .get_source(&workspace, &source.name)
                .await
                .expect("get source"),
            Some(original_source)
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
        let report = import_config_source_catalog(&db, &config_store, 22)
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
        let config_source = source("slack", None, [], [], None, SourceOrigin::Bundled);
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

        let report = import_config_source_catalog(&db, &config_store, 22)
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
    }

    #[tokio::test]
    async fn empty_config_catalog_import_is_noop() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let db = open_sqlite(&layout).await;

        let report = import_config_source_catalog(&db, &config_store, 11)
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
}
