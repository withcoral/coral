use std::collections::BTreeSet;

use super::session::DbRepos;
use super::{CoralDb, now_unix_nanos_i64};
use crate::bootstrap::AppError;
use crate::state::ConfigStore;
use crate::workspaces::WorkspaceRecord;

const WORKSPACE_CATALOG_CUTOVER_ID: &str = "workspace_catalog_cutover_v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LegacyConfigCutoverReport {
    pub(crate) workspace_count: usize,
    pub(crate) cutover_performed: bool,
}

pub(crate) async fn cutover_legacy_config(
    db: &CoralDb,
    config_store: &ConfigStore,
) -> Result<LegacyConfigCutoverReport, AppError> {
    cutover_legacy_config_at(db, config_store, now_unix_nanos_i64()?).await
}

async fn cutover_legacy_config_at(
    db: &CoralDb,
    config_store: &ConfigStore,
    now_unix_nanos: i64,
) -> Result<LegacyConfigCutoverReport, AppError> {
    let _state_lock = config_store.state_lock_exclusive()?;
    let mut session = db;
    if session
        .state_migrations()
        .has_completed(WORKSPACE_CATALOG_CUTOVER_ID)
        .await?
    {
        return Ok(LegacyConfigCutoverReport {
            workspace_count: session.workspaces().list().await?.len(),
            cutover_performed: false,
        });
    }

    let workspaces = config_store.load_config_unlocked()?.workspaces();
    let workspace_count = workspaces.len();

    let mut tx = db.begin().await?;
    tx.workspaces().delete_all().await?;
    import_legacy_workspaces(&mut tx, &workspaces, now_unix_nanos).await?;
    verify_workspace_parity(&mut tx, &workspaces).await?;
    tx.state_migrations()
        .mark_completed(WORKSPACE_CATALOG_CUTOVER_ID, now_unix_nanos)
        .await?;
    tx.commit().await?;

    Ok(LegacyConfigCutoverReport {
        workspace_count,
        cutover_performed: true,
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

async fn verify_workspace_parity<S>(
    session: &mut S,
    legacy_workspaces: &[WorkspaceRecord],
) -> Result<(), AppError>
where
    S: DbRepos,
{
    let expected = legacy_workspaces
        .iter()
        .map(|workspace| workspace.name.as_str().to_string())
        .collect::<BTreeSet<_>>();
    let actual = session
        .workspaces()
        .list()
        .await?
        .into_iter()
        .map(|workspace| workspace.id)
        .collect::<BTreeSet<_>>();
    if actual == expected {
        return Ok(());
    }
    Err(AppError::Database(format!(
        "workspace catalog cutover parity validation failed: legacy={expected:?} database={actual:?}"
    )))
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{
        LegacyConfigCutoverReport, WORKSPACE_CATALOG_CUTOVER_ID, cutover_legacy_config,
        cutover_legacy_config_at,
    };
    use crate::state::db::session::DbRepos;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig};
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::workspaces::WorkspaceName;

    #[tokio::test]
    async fn cuts_over_legacy_workspaces_into_database() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let analytics_workspace = WorkspaceName::parse("analytics").expect("workspace");
        config_store
            .create_workspace(&analytics_workspace)
            .expect("create workspace");
        let db = open_sqlite(&layout).await;

        let report = cutover_legacy_config_at(&db, &config_store, 11)
            .await
            .expect("cut over legacy config");

        assert_eq!(
            report,
            LegacyConfigCutoverReport {
                workspace_count: 2,
                cutover_performed: true
            }
        );
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
        assert!(
            session
                .state_migrations()
                .has_completed(WORKSPACE_CATALOG_CUTOVER_ID)
                .await
                .expect("read cutover marker")
        );
    }

    #[tokio::test]
    async fn cutover_resets_stale_shadow_database_rows() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let analytics_workspace = WorkspaceName::parse("analytics").expect("workspace");
        config_store
            .create_workspace(&analytics_workspace)
            .expect("create workspace");
        let db = open_sqlite(&layout).await;
        let mut tx = db.begin().await.expect("begin stale seed tx");
        tx.workspaces()
            .ensure("stale", 7)
            .await
            .expect("seed stale workspace");
        tx.commit().await.expect("commit stale seed tx");

        cutover_legacy_config_at(&db, &config_store, 11)
            .await
            .expect("cut over legacy config");

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

    #[tokio::test]
    async fn completed_cutover_does_not_reimport_legacy_config() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let db = open_sqlite(&layout).await;

        cutover_legacy_config_at(&db, &config_store, 11)
            .await
            .expect("initial cutover");
        std::fs::write(layout.config_file(), "[[workspaces]\n").expect("corrupt config");

        let report = cutover_legacy_config(&db, &config_store)
            .await
            .expect("marker should skip legacy config reload");

        assert_eq!(
            report,
            LegacyConfigCutoverReport {
                workspace_count: 1,
                cutover_performed: false
            }
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
