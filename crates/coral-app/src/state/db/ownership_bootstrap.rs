use std::io;
use std::path::Path;

use sea_query::{Expr, ExprTrait, Func, OnConflict, Query};

use super::schema::{IdentitySpecs, Users, WorkspaceMembers};
use super::{CoralDb, DbRepos, DbSession, now_unix_nanos_i64};
use crate::bootstrap::AppError;
use crate::state::{AppStateLayout, ConfigStore};
use crate::telemetry::{TelemetryConfig, has_retained_workspace_trace_attribution};
use crate::workspaces::{MemberRole, WorkspaceName};

const LOCAL_IDENTITY: &str = "coral:local";
const LOCAL_DISPLAY_NAME: &str = "Local";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OwnershipBootstrapPolicy {
    Local,
    MultiUser,
    Skip,
}

pub(crate) async fn bootstrap_workspace_ownership(
    db: &CoralDb,
    config_store: &ConfigStore,
    layout: &AppStateLayout,
    policy: OwnershipBootstrapPolicy,
) -> Result<(), AppError> {
    match policy {
        OwnershipBootstrapPolicy::Local => bootstrap_local(db).await,
        OwnershipBootstrapPolicy::MultiUser => enforce_multi_user(db, config_store, layout).await,
        OwnershipBootstrapPolicy::Skip => Ok(()),
    }
}

async fn bootstrap_local(db: &CoralDb) -> Result<(), AppError> {
    let now = now_unix_nanos_i64()?;
    let mut tx = db.begin().await?;
    let statement = Query::insert()
        .into_table(Users::Table)
        .columns([
            Users::UserId,
            Users::Issuer,
            Users::Subject,
            Users::DisplayName,
            Users::CreatedAtUnixNanos,
            Users::LastLoginAtUnixNanos,
        ])
        .values_panic([
            Expr::val(LOCAL_IDENTITY),
            Expr::val(LOCAL_IDENTITY),
            Expr::val(LOCAL_IDENTITY),
            Expr::val(LOCAL_DISPLAY_NAME),
            Expr::val(now),
            Expr::val(now),
        ])
        .on_conflict(
            OnConflict::column(Users::UserId)
                .update_columns([
                    Users::Issuer,
                    Users::Subject,
                    Users::DisplayName,
                    Users::LastLoginAtUnixNanos,
                ])
                .to_owned(),
        )
        .to_owned();
    DbSession::execute(&mut tx, statement).await?;

    for workspace in tx.workspaces().list().await? {
        if !tx
            .workspaces()
            .hold_for_child_mutation(&workspace.id)
            .await?
        {
            continue;
        }
        if tx.workspace_members().owner_count(&workspace.id).await? == 0 {
            tx.workspace_members()
                .delete(&workspace.id, LOCAL_IDENTITY)
                .await?;
            tx.workspace_members()
                .insert(&workspace.id, LOCAL_IDENTITY, MemberRole::Owner, now)
                .await?;
        }
    }
    tx.commit().await?;
    Ok(())
}

async fn enforce_multi_user(
    db: &CoralDb,
    config_store: &ConfigStore,
    layout: &AppStateLayout,
) -> Result<(), AppError> {
    let default = WorkspaceName::default();
    let mut tx = db.begin().await?;
    let mut ownerless = Vec::new();
    for workspace in tx.workspaces().list().await? {
        if tx
            .workspaces()
            .hold_for_child_mutation(&workspace.id)
            .await?
            && tx.workspace_members().owner_count(&workspace.id).await? == 0
        {
            ownerless.push(workspace.id);
        }
    }
    if ownerless.is_empty() {
        tx.commit().await?;
        return Ok(());
    }

    let default_is_sole_ownerless = ownerless.as_slice() == [default.as_str()];
    if default_is_sole_ownerless {
        let identity_spec_count = Query::select()
            .expr(Func::count(Expr::col(IdentitySpecs::Id)))
            .from(IdentitySpecs::Table)
            .and_where(Expr::col(IdentitySpecs::WorkspaceId).eq(default.as_str()))
            .to_owned();
        let (identity_spec_count,): (i64,) =
            DbSession::fetch_optional(&mut tx, identity_spec_count)
                .await?
                .unwrap_or_default();
        let membership_count = Query::select()
            .expr(Func::count(Expr::col(WorkspaceMembers::UserId)))
            .from(WorkspaceMembers::Table)
            .and_where(Expr::col(WorkspaceMembers::WorkspaceId).eq(default.as_str()))
            .to_owned();
        let (membership_count,): (i64,) = DbSession::fetch_optional(&mut tx, membership_count)
            .await?
            .unwrap_or_default();
        let db_pristine = tx.tasks().count(default.as_str()).await? == 0
            && identity_spec_count == 0
            && membership_count == 0;
        if db_pristine {
            let _state_lock = config_store.state_lock_shared()?;
            let config_pristine = config_store
                .load_config_unlocked()?
                .workspace_config_is_content_pristine(&default);
            let files_pristine = !directory_has_content(&layout.workspace_dir(&default))?;
            if config_pristine && files_pristine {
                let retention = TelemetryConfig::load(layout)?.trace_history.retention();
                let has_traces = has_retained_workspace_trace_attribution(
                    layout.local_trace_store_dir(),
                    retention,
                    &default,
                )
                .map_err(|error| {
                    AppError::FailedPrecondition(format!(
                        "could not verify local trace history before removing the default workspace: {error}"
                    ))
                })?;
                if !has_traces {
                    tx.workspaces().delete(default.as_str()).await?;
                    tx.commit().await?;
                    return Ok(());
                }
            }
        }
    }

    tx.rollback().await?;
    Err(AppError::FailedPrecondition(format!(
        "multi-user startup refused: ownerless workspaces require repair: {}. Start Coral locally against this state directory and add an owner before retrying",
        ownerless.join(", ")
    )))
}

fn directory_has_content(path: &Path) -> Result<bool, io::Error> {
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() || directory_has_content(&entry.path())? {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use tempfile::{TempDir, tempdir};

    use super::{
        LOCAL_DISPLAY_NAME, LOCAL_IDENTITY, OwnershipBootstrapPolicy,
        bootstrap_workspace_ownership, directory_has_content,
    };
    use crate::bootstrap;
    use crate::state::db::{
        CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig, UpsertLoginOutcome,
        now_unix_nanos_i64,
    };
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::workspaces::MemberRole;

    #[tokio::test]
    async fn ownership_bootstrap_contract_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("layout dirs");
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("db config")
        else {
            panic!("default test database must be sqlite");
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");

        assert_ownership_bootstrap_contract(&db, &layout).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the ownership bootstrap contract"]
    async fn ownership_bootstrap_contract_against_postgres() {
        let Some(url) = bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
            .expect("read CORAL_TEST_POSTGRES_URL")
            .filter(|value| !value.is_empty())
        else {
            return;
        };
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("layout dirs");
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");

        assert_ownership_bootstrap_contract(&db, &layout).await;
    }

    #[test]
    fn directory_has_content_handles_recursive_empty_missing_content_and_errors() {
        let temp = TempDir::new().expect("temp dir");
        let missing = temp.path().join("missing");
        assert!(!directory_has_content(&missing).expect("missing directory"));

        let nested = temp.path().join("empty").join("nested");
        std::fs::create_dir_all(&nested).expect("nested directories");
        assert!(!directory_has_content(temp.path()).expect("recursive empty directories"));

        let content = nested.join("content");
        std::fs::write(&content, "content").expect("nested content");
        assert!(directory_has_content(temp.path()).expect("recursive content"));
        assert!(directory_has_content(&content).is_err());
    }

    async fn assert_ownership_bootstrap_contract(db: &CoralDb, layout: &AppStateLayout) {
        let config_store = ConfigStore::new(layout.clone());
        let now = now_unix_nanos_i64().expect("system time");
        let mut tx = db.begin().await.expect("begin setup");
        tx.workspaces()
            .delete_all()
            .await
            .expect("clear workspaces");
        for workspace_id in ["default", "ownerless", "already_owned"] {
            tx.workspaces()
                .ensure(workspace_id, now)
                .await
                .expect("create workspace");
        }
        let owner = match tx
            .users()
            .upsert_login("issuer", "existing-owner", Some("Existing"), now)
            .await
            .expect("create existing owner")
        {
            UpsertLoginOutcome::Upserted(owner) => owner,
            UpsertLoginOutcome::IssuerMismatch { .. } => panic!("test issuer must match"),
        };
        tx.workspace_members()
            .insert("already_owned", &owner.user_id, MemberRole::Owner, now)
            .await
            .expect("add existing owner");
        tx.commit().await.expect("commit setup");

        bootstrap_workspace_ownership(db, &config_store, layout, OwnershipBootstrapPolicy::Local)
            .await
            .expect("bootstrap local ownership");
        let mut session = db;
        let local = session
            .users()
            .get_by_user_id(LOCAL_IDENTITY)
            .await
            .expect("load local user")
            .expect("local user");
        assert_eq!(local.issuer, LOCAL_IDENTITY);
        assert_eq!(local.subject, LOCAL_IDENTITY);
        assert_eq!(local.display_name.as_deref(), Some(LOCAL_DISPLAY_NAME));
        for workspace_id in ["default", "ownerless"] {
            assert_eq!(
                session
                    .workspace_members()
                    .role_for_user_id(workspace_id, LOCAL_IDENTITY)
                    .await
                    .expect("load local role"),
                Some(MemberRole::Owner)
            );
        }
        assert_eq!(
            session
                .workspace_members()
                .role_for_user_id("already_owned", LOCAL_IDENTITY)
                .await
                .expect("load local role"),
            None
        );

        let mut tx = db.begin().await.expect("begin pristine setup");
        tx.workspaces()
            .delete_all()
            .await
            .expect("clear workspaces");
        tx.workspaces()
            .ensure("default", now)
            .await
            .expect("create pristine default");
        tx.commit().await.expect("commit pristine setup");
        bootstrap_workspace_ownership(
            db,
            &config_store,
            layout,
            OwnershipBootstrapPolicy::MultiUser,
        )
        .await
        .expect("remove pristine default");
        assert!(
            session
                .workspaces()
                .get("default")
                .await
                .expect("load default")
                .is_none()
        );

        let mut tx = db.begin().await.expect("begin content setup");
        tx.workspaces()
            .ensure("default", now)
            .await
            .expect("create contentful default");
        tx.tasks()
            .insert("default", "principal", "task", "intent", now)
            .await
            .expect("insert workspace content");
        tx.commit().await.expect("commit content setup");
        let error = bootstrap_workspace_ownership(
            db,
            &config_store,
            layout,
            OwnershipBootstrapPolicy::MultiUser,
        )
        .await
        .expect_err("contentful ownerless default must fail");
        assert!(error.to_string().contains("default"));
        assert!(
            session
                .workspaces()
                .get("default")
                .await
                .expect("load retained default")
                .is_some()
        );

        let mut tx = db.begin().await.expect("begin member setup");
        tx.workspaces()
            .delete_all()
            .await
            .expect("clear workspaces");
        tx.workspaces()
            .ensure("default", now)
            .await
            .expect("create member-only default");
        tx.workspace_members()
            .insert("default", LOCAL_IDENTITY, MemberRole::Member, now)
            .await
            .expect("insert member-only membership");
        tx.commit().await.expect("commit member setup");
        let error = bootstrap_workspace_ownership(
            db,
            &config_store,
            layout,
            OwnershipBootstrapPolicy::MultiUser,
        )
        .await
        .expect_err("member-only default must fail");
        assert!(error.to_string().contains("default"));

        let mut tx = db.begin().await.expect("begin trace setup");
        tx.workspaces()
            .delete_all()
            .await
            .expect("clear workspaces");
        tx.workspaces()
            .ensure("default", now)
            .await
            .expect("create trace-only default");
        tx.commit().await.expect("commit trace setup");
        let trace_dir = layout.local_trace_store_dir();
        std::fs::create_dir_all(&trace_dir).expect("trace dir");
        let trace_file = trace_dir.join("spans-bootstrap.jsonl");
        let mixed_trace = concat!(
            r#"{"trace_id":"bootstrap-trace","span_id":"default","parent_span_id":null,"name":"root","start_time_unix_nanos":1,"end_time_unix_nanos":2,"attributes_json":"{\"workspace\":\"default\"}"}"#,
            "\n",
            r#"{"trace_id":"bootstrap-trace","span_id":"unattributed","parent_span_id":"default","name":"child","start_time_unix_nanos":1,"end_time_unix_nanos":2,"attributes_json":"{}"}"#,
            "\n"
        );
        std::fs::write(&trace_file, mixed_trace).expect("mixed trace records");
        let error = bootstrap_workspace_ownership(
            db,
            &config_store,
            layout,
            OwnershipBootstrapPolicy::MultiUser,
        )
        .await
        .expect_err("trace-only default must fail");
        assert!(error.to_string().contains("default"));

        std::fs::write(&trace_file, "{malformed}\n").expect("malformed complete trace record");
        let error = bootstrap_workspace_ownership(
            db,
            &config_store,
            layout,
            OwnershipBootstrapPolicy::MultiUser,
        )
        .await
        .expect_err("trace inspection failure must fail closed");
        assert!(
            error
                .to_string()
                .contains("could not verify local trace history")
        );
        assert!(
            session
                .workspaces()
                .get("default")
                .await
                .expect("load retained trace-only default")
                .is_some()
        );
    }
}
