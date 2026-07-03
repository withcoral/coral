//! RDBMS-backed durable app-state infrastructure.

mod backend;
mod clock;
mod config;
mod coral_db;
mod error;
mod import;
mod migrations;
mod ownership_bootstrap;
mod repositories;
mod schema;
mod session;
mod task_query_state;
mod task_state;
mod trace_search_response_state;
mod transaction;
mod user_state;
mod workspace_state;

pub(crate) use crate::telemetry::TraceSummaryRecord;
pub(crate) use clock::now_unix_nanos_i64;
pub(crate) use config::{DatabaseConfig, ResolvedDatabaseConfig};
pub(crate) use coral_db::CoralDb;
pub(crate) use error::DbError;
pub(crate) use import::{
    import_filesystem_feedback_reports, import_legacy_credential_material, run_state_migrations,
};
pub(crate) use ownership_bootstrap::{inaccessible_workspaces, migrate_local_ownership_once};
pub(crate) use repositories::credential_documents::{
    CredentialDocumentRecord, CredentialDocumentWrite,
};
pub(crate) use repositories::feedback_reports::FeedbackReportRecord;
pub(crate) use repositories::identity_specs::{
    IdentitySpecDocumentRecord, IdentitySpecId, IdentitySpecKey, IdentitySpecRecord,
    IdentitySpecScope,
};
pub(crate) use repositories::materializations::{
    MaterializationRecord, MaterializationSurfaceRecord,
};
#[cfg(test)]
pub(crate) use repositories::state_migrations::LOCAL_WORKSPACE_OWNERSHIP_MIGRATION_ID;
pub(crate) use repositories::tasks::{TaskCompletionUpdate, TaskLifecycleState};
pub(crate) use repositories::workspaces::InaccessibleWorkspaces;
pub(crate) use session::{DbRepos, DbSession};
#[cfg(test)]
pub(crate) use task_query_state::TaskQueryRelationRecord;
pub(crate) use task_query_state::{TaskQueryRelationWrite, TaskQueryWrite, TaskQueryWriteResult};
#[cfg(test)]
pub(crate) use task_state::TaskMutationBarrier;
pub(crate) use task_state::{TaskCreation, TaskCreationResult};
pub(crate) use transaction::CoralTx;
pub(crate) use user_state::{LoginIdentity, LoginProvisioning};
pub(crate) use workspace_state::{
    AddMemberOutcome, CreateWorkspaceOutcome, RemoveMemberOutcome, WorkspaceDeletion,
    WorkspaceMemberRecord,
};

#[cfg(test)]
pub(crate) async fn open_test_database(
    layout: &super::AppStateLayout,
) -> Result<std::sync::Arc<CoralDb>, crate::bootstrap::AppError> {
    let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(layout)? else {
        return Err(crate::bootstrap::AppError::FailedPrecondition(
            "default test database config should use SQLite".to_string(),
        ));
    };
    let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path }).await?;
    db.migrate().await?;
    Ok(std::sync::Arc::new(db))
}
