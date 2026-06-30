//! RDBMS-backed durable app-state infrastructure.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Phase 1 lands the DB boundary and repository harness before later PRs wire managers to these repositories."
    )
)]

mod backend;
mod config;
mod coral_db;
mod error;
mod import;
mod migrations;
mod repositories;
mod schema;
mod session;
mod transaction;

pub(crate) use crate::telemetry::TraceSummaryRecord;
pub(crate) use config::{DatabaseConfig, ResolvedDatabaseConfig};
pub(crate) use coral_db::CoralDb;
pub(crate) use error::DbError;
pub(crate) use import::{
    import_config_source_catalog, import_filesystem_episodes, import_filesystem_feedback_reports,
    import_legacy_credential_material,
};
pub(crate) use repositories::credential_documents::{
    CredentialDocumentRecord, CredentialDocumentWrite,
};
pub(crate) use repositories::episodes::EpisodeRecord;
pub(crate) use repositories::feedback_reports::FeedbackReportRecord;
pub(crate) use repositories::materializations::{
    MaterializationRecord, MaterializationSurfaceRecord,
};
pub(crate) use session::{DbRepos, DbSession, DbWriteSession};
pub(crate) use transaction::CoralTx;
