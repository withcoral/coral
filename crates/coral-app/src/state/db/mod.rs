//! RDBMS-backed durable app-state infrastructure.

mod backend;
mod clock;
mod config;
mod coral_db;
mod error;
mod import;
mod migrations;
mod repositories;
mod schema;
mod session;
mod task_state;
mod transaction;
mod user_state;
mod workspace_member_state;
mod workspace_state;

pub(crate) use clock::now_unix_nanos_i64;
pub(crate) use config::{DatabaseConfig, ResolvedDatabaseConfig};
pub(crate) use coral_db::CoralDb;
pub(crate) use error::DbError;
pub(crate) use import::run_state_migrations;
#[expect(
    unused_imports,
    reason = "identity persistence types are not yet wired to production consumers"
)]
pub(crate) use repositories::identity_specs::{
    IdentitySpecDocumentRecord, IdentitySpecKey, IdentitySpecRecord, IdentitySpecScope,
};
pub(crate) use repositories::tasks::{TaskCompletionUpdate, TaskLifecycleState};
#[expect(
    unused_imports,
    reason = "user persistence types are wired to production consumers in M2"
)]
pub(crate) use repositories::users::{UpsertLoginOutcome, UserRecord};
pub(crate) use session::{DbRepos, DbSession};
#[cfg(test)]
pub(crate) use task_state::TaskMutationBarrier;
pub(crate) use task_state::{TaskCreation, TaskCreationResult};
pub(crate) use transaction::CoralTx;
pub(crate) use workspace_member_state::{
    AddMemberOutcome, RemoveMemberOutcome, WorkspaceMemberView,
};
pub(crate) use workspace_state::WorkspaceCreationOutcome;
