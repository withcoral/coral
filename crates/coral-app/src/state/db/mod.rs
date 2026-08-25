//! RDBMS-backed durable app-state infrastructure.

mod backend;
mod clock;
mod config;
mod coral_db;
mod error;
mod identity_spec_state;
mod import;
mod migrations;
mod repositories;
mod schema;
mod session;
mod task_query_state;
mod task_state;
mod trace_search_response_state;
mod transaction;
mod workspace_state;

pub(crate) use clock::now_unix_nanos_i64;
pub(crate) use config::{DatabaseConfig, ResolvedDatabaseConfig};
pub(crate) use coral_db::CoralDb;
pub(crate) use error::DbError;
#[cfg(test)]
pub(crate) use identity_spec_state::IdentitySpecMutationSnapshot;
pub(crate) use import::run_state_migrations;
pub(crate) use repositories::identity_specs::{
    IdentitySpecDocumentRecord, IdentitySpecId, IdentitySpecKey, IdentitySpecRecord,
    IdentitySpecScope,
};
#[cfg(test)]
pub(crate) use repositories::identity_specs_contract_tests::set_identity_spec_document_version;
pub(crate) use repositories::tasks::{TaskCompletionUpdate, TaskLifecycleState};
pub(crate) use session::{DbRepos, DbSession};
#[cfg(test)]
pub(crate) use task_query_state::TaskQueryRelationRecord;
pub(crate) use task_query_state::{TaskQueryRelationWrite, TaskQueryWrite, TaskQueryWriteResult};
#[cfg(test)]
pub(crate) use task_state::TaskMutationBarrier;
pub(crate) use task_state::{TaskCreation, TaskCreationResult};
pub(crate) use transaction::CoralTx;
