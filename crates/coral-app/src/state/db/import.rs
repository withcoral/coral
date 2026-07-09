use super::session::DbRepos;
use super::{CoralDb, now_unix_nanos_i64};
use crate::bootstrap::AppError;
use crate::state::ConfigStore;
use crate::workspaces::WorkspaceRecord;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LegacyConfigImportReport {
    pub(crate) workspace_count: usize,
}

/// Backfills the workspace database shadow from the filesystem-backed config.
///
/// Workspace reads still use `config.toml`; startup treats this as a rehearsal
/// import and logs failures instead of making the database authoritative.
pub(crate) async fn import_legacy_config(
    db: &CoralDb,
    config_store: &ConfigStore,
) -> Result<LegacyConfigImportReport, AppError> {
    import_legacy_config_at(db, config_store, now_unix_nanos_i64()?).await
}

async fn import_legacy_config_at(
    db: &CoralDb,
    config_store: &ConfigStore,
    now_unix_nanos: i64,
) -> Result<LegacyConfigImportReport, AppError> {
    let _state_lock = config_store.state_lock_shared()?;
    let workspaces = config_store.load_config_unlocked()?.workspaces();

    let mut tx = db.begin().await?;
    import_legacy_workspaces(&mut tx, &workspaces, now_unix_nanos).await?;
    tx.commit().await?;

    Ok(LegacyConfigImportReport {
        workspace_count: workspaces.len(),
    })
}

async fn import_legacy_workspaces<S>(
    session: &mut S,
    workspaces: &[WorkspaceRecord],
    now_unix_nanos: i64,
) -> Result<(), AppError>
where
    S: DbRepos,
{
    for workspace in workspaces {
        session
            .workspaces()
            .ensure(workspace.name.as_str(), now_unix_nanos)
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{LegacyConfigImportReport, import_legacy_config_at};
    use crate::state::db::session::DbRepos;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig};
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::workspaces::WorkspaceName;

    #[tokio::test]
    async fn imports_legacy_workspaces_into_database() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let analytics_workspace = WorkspaceName::parse("analytics").expect("workspace");
        config_store
            .create_workspace(&analytics_workspace)
            .expect("create workspace");
        let db = open_sqlite(&layout).await;

        let report = import_legacy_config_at(&db, &config_store, 11)
            .await
            .expect("import legacy config");

        assert_eq!(report, LegacyConfigImportReport { workspace_count: 2 });
        let mut session = &db;
        assert_eq!(
            session
                .workspaces()
                .list()
                .await
                .expect("list workspaces")
                .into_iter()
                .map(|workspace| workspace.id)
                .collect::<Vec<_>>(),
            vec![
                "analytics".to_string(),
                WorkspaceName::default().as_str().to_string(),
            ]
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
}
