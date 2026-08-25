use std::collections::BTreeSet;

use super::session::DbRepos;
use super::{CoralDb, now_unix_nanos_i64};
use crate::bootstrap::AppError;
use crate::state::{AppStateLayout, ConfigStore};
use crate::workspaces::{WorkspaceName, WorkspaceRecord};

const WORKSPACE_CATALOG_CUTOVER_ID: &str = "workspace_catalog_cutover_v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkspaceCatalogCutoverReport {
    pub(crate) workspace_count: usize,
    pub(crate) cutover_performed: bool,
}

pub(crate) async fn run_state_migrations(
    db: &CoralDb,
    config_store: &ConfigStore,
    layout: &AppStateLayout,
) -> Result<(), AppError> {
    cutover_legacy_workspace_catalog(db, config_store, layout).await?;
    remove_legacy_task_jsonl(config_store, layout)?;
    Ok(())
}

fn remove_legacy_task_jsonl(
    config_store: &ConfigStore,
    layout: &AppStateLayout,
) -> Result<(), AppError> {
    let _state_lock = config_store.state_lock_exclusive()?;
    layout.remove_legacy_task_event_logs()?;
    Ok(())
}

async fn cutover_legacy_workspace_catalog(
    db: &CoralDb,
    config_store: &ConfigStore,
    layout: &AppStateLayout,
) -> Result<WorkspaceCatalogCutoverReport, AppError> {
    cutover_legacy_workspace_catalog_at(db, config_store, layout, now_unix_nanos_i64()?).await
}

async fn cutover_legacy_workspace_catalog_at(
    db: &CoralDb,
    config_store: &ConfigStore,
    layout: &AppStateLayout,
    now_unix_nanos: i64,
) -> Result<WorkspaceCatalogCutoverReport, AppError> {
    let _state_lock = config_store.state_lock_exclusive()?;
    let mut tx = db.begin().await?;
    if !tx
        .state_migrations()
        .try_claim(WORKSPACE_CATALOG_CUTOVER_ID, now_unix_nanos)
        .await?
    {
        tx.rollback().await?;
        let mut session = db;
        return Ok(WorkspaceCatalogCutoverReport {
            workspace_count: session.workspaces().list().await?.len(),
            cutover_performed: false,
        });
    }

    let mut workspaces = config_store
        .load_config_unlocked()?
        .legacy_workspace_records();
    if workspaces.is_empty() {
        workspaces = implicitly_provisioned_workspaces(layout)?;
    }
    let workspace_count = workspaces.len();

    tx.workspaces().delete_all().await?;
    import_legacy_workspaces(&mut tx, &workspaces, now_unix_nanos).await?;
    verify_workspace_parity(&mut tx, &workspaces).await?;
    tx.commit().await?;

    Ok(WorkspaceCatalogCutoverReport {
        workspace_count,
        cutover_performed: true,
    })
}

/// Lists the workspaces on-disk state proves an install had, for a legacy
/// config that names none itself.
///
/// Workspaces were once implicit: the catalog seeded one, so a `config.toml`
/// with no workspace tables still described an install that had a workspace,
/// with sources, tasks, and search state under its directory. Nothing records
/// that name any more except the directory itself, so the cutover reads it
/// from there. It does not fall back to a fixed `default`: a genuinely fresh
/// install has no workspace directory and must cut over to no workspaces.
///
/// Reading a directory as a live workspace is sound because deletion moves the
/// directory out of the workspaces root before it commits anything, so this
/// root holds live workspaces only. That holds for deletions this version
/// performed. A directory an older Coral orphaned — it staged the deletion
/// beside the live workspaces and failed to remove it, or failed to stage it
/// at all after committing — carries no evidence of being deleted, and this
/// scan does resurrect it. Nothing on disk can tell the two apart after the
/// fact; the residue is bounded by installs that hit that failure before
/// upgrading.
///
/// Deliberately a fallback and not a union with the config's own names, which
/// leaves one residual open. A config that named `analytics` and nothing else
/// could still have had a live implicit workspace beside it, and that one is
/// orphaned for good because the cutover marker never re-runs. Closing it by
/// unioning would resurrect exactly the older-Coral orphans described above,
/// and `cuts_over_legacy_workspaces_into_database` pins that a leftover
/// directory must not come back beside a config that names workspaces. The
/// exposed population is narrow: every config Coral itself persisted
/// serializes its workspaces back, so only a hand-edited config reaches this
/// shape.
fn implicitly_provisioned_workspaces(
    layout: &AppStateLayout,
) -> Result<Vec<WorkspaceRecord>, AppError> {
    let entries = match std::fs::read_dir(layout.workspaces_root()) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };

    let mut workspaces = Vec::new();
    for entry in entries {
        let entry = entry?;
        // Metadata, not `file_type`: a workspace directory may be a symlink to
        // another volume, and skipping it here would orphan it for good once
        // the cutover marker commits. Following the link means a dangling one
        // reports `NotFound`, which is this scan's answer for "not a workspace
        // directory" rather than a reason to fail startup; every other io error
        // still surfaces.
        match std::fs::metadata(entry.path()) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => continue,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        }
        let directory_name = entry.file_name();
        let Some(name) = directory_name
            .to_str()
            .and_then(|name| WorkspaceName::parse(name).ok())
        else {
            continue;
        };
        workspaces.push(WorkspaceRecord { name });
    }
    workspaces.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(workspaces)
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
        WORKSPACE_CATALOG_CUTOVER_ID, WorkspaceCatalogCutoverReport,
        cutover_legacy_workspace_catalog, cutover_legacy_workspace_catalog_at,
        run_state_migrations,
    };
    use crate::state::db::session::DbRepos;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig};
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::storage::fs::DELETION_BACKUP_INFIX;
    use crate::workspaces::WorkspaceName;

    /// The unique suffix a staged deletion carries, fixed so the directories
    /// these tests plant read exactly as `move_for_delete` would have written
    /// them.
    const STAGED_DELETION_SUFFIX: &str = "7f1c5a4e-1d29-4f3a-9f2b-2c6d0f9a1b34";

    #[tokio::test]
    async fn cuts_over_legacy_workspaces_into_database() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let analytics_workspace = WorkspaceName::parse("analytics").expect("workspace");
        config_store
            .create_legacy_workspace_entry_for_tests(&analytics_workspace)
            .expect("create legacy workspace entry");
        // A config that names workspaces is authoritative, so a directory left
        // behind by a deleted workspace must not come back beside them.
        let deleted_workspace = WorkspaceName::parse("removed").expect("workspace");
        std::fs::create_dir_all(layout.workspace_dir(&deleted_workspace))
            .expect("create leftover workspace dir");
        let db = open_sqlite(&layout).await;

        let report = cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 11)
            .await
            .expect("cut over legacy workspace catalog");

        assert_eq!(
            report,
            WorkspaceCatalogCutoverReport {
                workspace_count: 1,
                cutover_performed: true
            }
        );
        assert_eq!(
            workspace_ids(&db).await,
            vec!["analytics".to_string()],
            "the cutover carries the legacy names across and invents none"
        );
        let mut session = &db;
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
            .create_legacy_workspace_entry_for_tests(&analytics_workspace)
            .expect("create legacy workspace entry");
        let db = open_sqlite(&layout).await;
        let mut tx = db.begin().await.expect("begin stale seed tx");
        tx.workspaces()
            .ensure("stale", 7)
            .await
            .expect("seed stale workspace");
        tx.commit().await.expect("commit stale seed tx");

        cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 11)
            .await
            .expect("cut over legacy workspace catalog");

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
            vec!["analytics".to_string()]
        );
    }

    #[tokio::test]
    async fn completed_cutover_does_not_reimport_legacy_config() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let db = open_sqlite(&layout).await;

        cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 11)
            .await
            .expect("initial cutover");
        std::fs::write(layout.config_file(), "[[workspaces]\n").expect("corrupt config");

        let report = cutover_legacy_workspace_catalog(&db, &config_store, &layout)
            .await
            .expect("marker should skip legacy config reload");

        assert_eq!(
            report,
            WorkspaceCatalogCutoverReport {
                workspace_count: 0,
                cutover_performed: false
            }
        );
    }

    /// A fresh install names no workspace and holds none on disk, so the
    /// cutover must not seed one on its way into the database.
    #[tokio::test]
    async fn cutover_without_legacy_workspaces_creates_none() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        // An install that has held a workspace and lost it looks the same as
        // one that never had one: an emptied root, and at most a directory a
        // deletion staged aside — into its own root — and failed to remove.
        std::fs::create_dir_all(layout.workspaces_root()).expect("create workspaces root");
        std::fs::create_dir_all(layout.deleted_workspaces_root().join(format!(
            "default{DELETION_BACKUP_INFIX}{STAGED_DELETION_SUFFIX}"
        )))
        .expect("stage a deletion that was never removed");
        let config_store = ConfigStore::new(layout.clone());
        let db = open_sqlite(&layout).await;

        let report = cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 11)
            .await
            .expect("cut over legacy workspace catalog");

        assert_eq!(
            report,
            WorkspaceCatalogCutoverReport {
                workspace_count: 0,
                cutover_performed: true
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

    /// Workspaces were once implicit, so an older install can hold one whose
    /// name only its directory records. The cutover happens once and marks
    /// itself done, so a workspace it drops is orphaned for good: its name and
    /// its contents have to come across.
    #[tokio::test]
    async fn cutover_preserves_an_implicitly_provisioned_legacy_workspace() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let implicit_workspace = WorkspaceName::parse("default").expect("workspace");
        let installed_source = layout
            .sources_root(&implicit_workspace)
            .join("github")
            .join("manifest.yaml");
        std::fs::create_dir_all(installed_source.parent().expect("source dir"))
            .expect("create legacy source dir");
        std::fs::write(&installed_source, "name: github").expect("write legacy manifest");
        let db = open_sqlite(&layout).await;

        let report = cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 11)
            .await
            .expect("cut over legacy workspace catalog");

        assert_eq!(
            report,
            WorkspaceCatalogCutoverReport {
                workspace_count: 1,
                cutover_performed: true
            }
        );
        assert_eq!(workspace_ids(&db).await, vec!["default".to_string()]);
        assert!(
            installed_source.exists(),
            "the preserved workspace keeps its contents"
        );
    }

    /// A workspace directory can be a symlink — relocated to another volume by
    /// hand, say. The cutover runs once, so reading only the link itself and
    /// not what it points at would orphan that workspace for good.
    #[cfg(unix)]
    #[tokio::test]
    async fn cutover_preserves_a_symlinked_legacy_workspace() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let implicit_workspace = WorkspaceName::parse("default").expect("workspace");
        let relocated = temp.path().join("other-volume").join("default");
        std::fs::create_dir_all(&relocated).expect("create relocated workspace dir");
        std::fs::create_dir_all(layout.workspaces_root()).expect("create workspaces root");
        std::os::unix::fs::symlink(&relocated, layout.workspace_dir(&implicit_workspace))
            .expect("link the relocated workspace into place");
        let db = open_sqlite(&layout).await;

        let report = cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 11)
            .await
            .expect("cut over legacy workspace catalog");

        assert_eq!(
            report,
            WorkspaceCatalogCutoverReport {
                workspace_count: 1,
                cutover_performed: true
            }
        );
        assert_eq!(workspace_ids(&db).await, vec!["default".to_string()]);
    }

    /// Following the link is what carries a relocated workspace across, and a
    /// link whose target is gone is the cost of that. It names no workspace, so
    /// the cutover skips it — refusing to start over a broken link would strand
    /// the whole install.
    #[cfg(unix)]
    #[tokio::test]
    async fn cutover_skips_a_dangling_workspace_symlink() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let live_workspace = WorkspaceName::parse("analytics").expect("workspace");
        let dangling_workspace = WorkspaceName::parse("relocated").expect("workspace");
        std::fs::create_dir_all(layout.workspace_dir(&live_workspace))
            .expect("create the live workspace dir");
        std::os::unix::fs::symlink(
            temp.path().join("other-volume").join("relocated"),
            layout.workspace_dir(&dangling_workspace),
        )
        .expect("link a workspace that is no longer there");
        let db = open_sqlite(&layout).await;

        let report = cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 11)
            .await
            .expect("cut over legacy workspace catalog");

        assert_eq!(
            report,
            WorkspaceCatalogCutoverReport {
                workspace_count: 1,
                cutover_performed: true
            }
        );
        assert_eq!(workspace_ids(&db).await, vec!["analytics".to_string()]);
    }

    /// A workspace name is free to look exactly like a staged deletion, so a
    /// name can never say which of the two a directory is. Location can: the
    /// live workspace stays in the workspaces root and the staged deletion
    /// sits in its own, and only the former is imported.
    #[tokio::test]
    async fn cutover_separates_a_staged_deletion_from_a_workspace_named_like_one() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let named_like_a_backup =
            format!("analytics{DELETION_BACKUP_INFIX}{STAGED_DELETION_SUFFIX}");
        let implicit_workspace = WorkspaceName::parse(&named_like_a_backup).expect("workspace");
        std::fs::create_dir_all(layout.workspace_dir(&implicit_workspace))
            .expect("create the legacy workspace dir");
        std::fs::create_dir_all(layout.deleted_workspaces_root().join(format!(
            "work{DELETION_BACKUP_INFIX}{STAGED_DELETION_SUFFIX}"
        )))
        .expect("stage a deletion outside the workspaces root");
        let db = open_sqlite(&layout).await;

        cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 11)
            .await
            .expect("cut over legacy workspace catalog");

        assert_eq!(workspace_ids(&db).await, vec![named_like_a_backup]);
    }

    async fn workspace_ids(db: &CoralDb) -> Vec<String> {
        let mut session = db;
        session
            .workspaces()
            .list()
            .await
            .expect("list workspaces")
            .into_iter()
            .map(|workspace| workspace.id)
            .collect()
    }

    #[tokio::test]
    async fn shared_database_does_not_scope_task_cleanup_to_the_first_layout() {
        let temp = tempdir().expect("temp dir");
        let first_layout =
            AppStateLayout::discover(Some(temp.path().join("first"))).expect("first layout");
        let second_layout =
            AppStateLayout::discover(Some(temp.path().join("second"))).expect("second layout");
        first_layout.ensure().expect("ensure first layout");
        second_layout.ensure().expect("ensure second layout");
        let first_config_store = ConfigStore::new(first_layout.clone());
        let second_config_store = ConfigStore::new(second_layout.clone());
        let legacy_workspace = WorkspaceName::parse("analytics").expect("workspace");
        let first_legacy_file = first_layout
            .workspace_dir(&legacy_workspace)
            .join("tasks")
            .join("tasks.jsonl");
        let second_legacy_file = second_layout
            .workspace_dir(&legacy_workspace)
            .join("tasks")
            .join("tasks.jsonl");
        for path in [&first_legacy_file, &second_legacy_file] {
            std::fs::create_dir_all(path.parent().expect("legacy task dir"))
                .expect("create legacy task dir");
            std::fs::write(path, "sensitive task intent").expect("write legacy task file");
        }
        let db = open_sqlite(&first_layout).await;

        run_state_migrations(&db, &first_config_store, &first_layout)
            .await
            .expect("run migrations for first layout");
        run_state_migrations(&db, &second_config_store, &second_layout)
            .await
            .expect("run migrations for second layout");

        assert!(!first_legacy_file.exists());
        assert!(!second_legacy_file.exists());
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
