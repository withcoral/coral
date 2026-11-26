//! The accepted Program package, its linked source snapshots, and `run.started`.

use coral_catalog::AuthoredToolKind;
use coral_db::Transaction;
use coral_tools::LinkedToolSelection;
use sea_query::{Expr, Iden, OnConflict, Query};
use sqlx::FromRow;

use super::events::{NewEvent, insert_events};
use super::{ChunkEvent, ProgramRunRecords};
use crate::engine::ProgramPackage;
use crate::types::AcceptedRun;
use crate::{
    EMITTED_JAVASCRIPT_MAX_BYTES, Error, PROGRAM_SOURCE_MAX_BYTES, ProgramSource, RunState,
    SOURCE_MAP_MAX_BYTES,
};

/// The shutdown grace the previous release wrote on every Run, 5 s.
const PREVIOUS_RELEASE_SHUTDOWN_GRACE_NANOS: i64 = 5_000_000_000;

#[cfg(test)]
mod tests;

#[derive(Iden, Clone, Copy)]
pub(super) enum ProgramRunPackages {
    Table,
    RunId,
    WorkspaceId,
    SubmittedSource,
    EmittedJavascript,
    SourceMap,
    CompilerDiagnosticsJson,
    TypescriptProfileId,
    CompilerVersion,
    JavascriptTarget,
    ProgramSourceMaxBytes,
    EmittedJavascriptMaxBytes,
    SourceMapMaxBytes,
    ProgramValueMaxBytes,
    CompilationTimeoutMillis,
}

/// The package columns a read decodes.
pub(super) const PACKAGE_COLUMNS: [ProgramRunPackages; 6] = [
    ProgramRunPackages::EmittedJavascript,
    ProgramRunPackages::SourceMap,
    ProgramRunPackages::CompilerDiagnosticsJson,
    ProgramRunPackages::TypescriptProfileId,
    ProgramRunPackages::CompilerVersion,
    ProgramRunPackages::JavascriptTarget,
];

#[derive(Debug, FromRow)]
pub(super) struct PackageRow {
    emitted_javascript: String,
    source_map: String,
    compiler_diagnostics_json: String,
    typescript_profile_id: String,
    compiler_version: String,
    javascript_target: String,
}

impl PackageRow {
    pub(super) fn decode(self) -> Result<ProgramPackage, Error> {
        let package = ProgramPackage::new(
            self.emitted_javascript,
            self.source_map,
            self.compiler_diagnostics_json,
            self.typescript_profile_id,
            self.compiler_version,
            self.javascript_target,
        );
        package.validate()?;
        Ok(package)
    }
}

/// Inserts the Run row, unclaimed and with no deadline (its start records
/// one), and its package, which records the `compilation_timeout` it was
/// compiled under. The Run waits for a take. `false` means a Run with this
/// identifier already exists and nothing was written: the caller draws a new
/// identifier.
pub(crate) async fn insert_acceptance(
    transaction: &mut Transaction<'_>,
    run: &AcceptedRun,
    source: &ProgramSource,
    package: &ProgramPackage,
    linked: &[LinkedToolSelection],
    compilation_timeout: std::time::Duration,
) -> Result<bool, Error> {
    let program_source_max_bytes = stored_usize(PROGRAM_SOURCE_MAX_BYTES)?;
    let emitted_javascript_max_bytes = stored_usize(EMITTED_JAVASCRIPT_MAX_BYTES)?;
    let source_map_max_bytes = stored_usize(SOURCE_MAP_MAX_BYTES)?;
    let program_value_max_bytes = stored_usize(run.value_limits.host_bytes())?;
    let compilation_timeout_millis =
        i64::try_from(compilation_timeout.as_millis()).map_err(|_error| {
            Error::CorruptData("compilation timeout exceeds durable range".to_string())
        })?;
    if !insert_record(transaction, run).await? {
        return Ok(false);
    }
    transaction.ring_on_commit(super::QUEUE);

    let statement = Query::insert()
        .into_table(ProgramRunPackages::Table)
        .columns([
            ProgramRunPackages::RunId,
            ProgramRunPackages::WorkspaceId,
            ProgramRunPackages::SubmittedSource,
            ProgramRunPackages::EmittedJavascript,
            ProgramRunPackages::SourceMap,
            ProgramRunPackages::CompilerDiagnosticsJson,
            ProgramRunPackages::TypescriptProfileId,
            ProgramRunPackages::CompilerVersion,
            ProgramRunPackages::JavascriptTarget,
            ProgramRunPackages::ProgramSourceMaxBytes,
            ProgramRunPackages::EmittedJavascriptMaxBytes,
            ProgramRunPackages::SourceMapMaxBytes,
            ProgramRunPackages::ProgramValueMaxBytes,
            ProgramRunPackages::CompilationTimeoutMillis,
        ])
        .values_panic([
            Expr::val(run.id.to_string()),
            Expr::val(run.workspace_id.to_string()),
            Expr::val(source.as_str()),
            Expr::val(&package.javascript),
            Expr::val(&package.source_map),
            Expr::val(&package.diagnostics_json),
            Expr::val(&package.typescript_profile_id),
            Expr::val(&package.compiler_version),
            Expr::val(&package.javascript_target),
            Expr::val(program_source_max_bytes),
            Expr::val(emitted_javascript_max_bytes),
            Expr::val(source_map_max_bytes),
            Expr::val(program_value_max_bytes),
            Expr::val(compilation_timeout_millis),
        ])
        .to_owned();
    transaction.execute(statement).await?;
    for snapshot in linked {
        match snapshot.kind() {
            AuthoredToolKind::Private => insert_run_snapshot(transaction, run, snapshot).await?,
            AuthoredToolKind::Shared => {
                insert_shared_run_snapshot(transaction, run, snapshot).await?;
            }
        }
    }

    // Acceptance creates the Run Ledger at head 0 with its first Event. There
    // is no execution claim to predicate on yet; the row insert itself is the
    // write fence.
    insert_events(
        transaction,
        &run.workspace_id,
        &run.id,
        &[ChunkEvent {
            event: NewEvent::Started,
            occurred_at_unix_nanos: run.accepted_at_unix_nanos,
        }],
    )
    .await?;
    Ok(true)
}

/// Inserts the Run row, which waits for a take: no claim and no holder.
/// `false` means a Run with this identifier already exists and nothing was
/// written. The conflict target is the primary key, so a Postgres
/// transaction stays usable after it.
async fn insert_record(
    transaction: &mut Transaction<'_>,
    run: &AcceptedRun,
) -> Result<bool, Error> {
    let statement = Query::insert()
        .into_table(ProgramRunRecords::Table)
        .columns([
            ProgramRunRecords::Id,
            ProgramRunRecords::WorkspaceId,
            ProgramRunRecords::PrincipalId,
            ProgramRunRecords::PrincipalKind,
            ProgramRunRecords::State,
            ProgramRunRecords::AcceptedAtUnixNanos,
            ProgramRunRecords::ShutdownGraceNanos,
            ProgramRunRecords::TraceParent,
            ProgramRunRecords::ValueLimitsJson,
            ProgramRunRecords::NextEventSequence,
            ProgramRunRecords::AccumulatedOutputBytes,
            ProgramRunRecords::ChunkSequence,
            ProgramRunRecords::TimeLimitNanos,
        ])
        .values_panic([
            Expr::val(run.id.to_string()),
            Expr::val(run.workspace_id.to_string()),
            Expr::val(run.principal.id().as_str()),
            Expr::val(run.principal.kind().as_storage_str()),
            Expr::val(RunState::Running.as_storage_str()),
            Expr::val(run.accepted_at_unix_nanos),
            // The previous release's shutdown grace, which its row decode
            // requires positive: this release never reads it.
            Expr::val(PREVIOUS_RELEASE_SHUTDOWN_GRACE_NANOS),
            Expr::val(run.trace.as_ref().map(|trace| trace.as_str().to_string())),
            Expr::val(serde_json::to_string(&run.value_limits).map_err(Error::ProgramValueJson)?),
            // The previous release's Event counter: this release advances it per
            // chunk for that release's writers and never reads it.
            Expr::val(2_i64),
            Expr::val(0_i64),
            Expr::val(0_i64),
            Expr::val(run.time_limit_nanos),
        ])
        .on_conflict(
            OnConflict::column(ProgramRunRecords::Id)
                .do_nothing()
                .to_owned(),
        )
        .to_owned();
    Ok(transaction.execute(statement).await? == 1)
}

fn stored_revision(snapshot: &LinkedToolSelection) -> Result<i64, Error> {
    i64::try_from(snapshot.revision())
        .map_err(|_error| Error::CorruptData("Tool revision exceeds durable range".to_string()))
}

fn stored_usize(value: usize) -> Result<i64, Error> {
    i64::try_from(value)
        .map_err(|_error| Error::CorruptData("Program limit exceeds durable range".to_string()))
}

#[derive(Iden, Clone, Copy)]
pub(super) enum ProgramRunPrivateToolSnapshots {
    Table,
    RunId,
    WorkspaceId,
    PrivateToolId,
    Path,
    Source,
    Revision,
    DefaultAccessState,
}

async fn insert_run_snapshot(
    transaction: &mut Transaction<'_>,
    run: &AcceptedRun,
    snapshot: &LinkedToolSelection,
) -> Result<(), Error> {
    let statement = Query::insert()
        .into_table(ProgramRunPrivateToolSnapshots::Table)
        .columns([
            ProgramRunPrivateToolSnapshots::RunId,
            ProgramRunPrivateToolSnapshots::WorkspaceId,
            ProgramRunPrivateToolSnapshots::PrivateToolId,
            ProgramRunPrivateToolSnapshots::Path,
            ProgramRunPrivateToolSnapshots::Source,
            ProgramRunPrivateToolSnapshots::Revision,
            ProgramRunPrivateToolSnapshots::DefaultAccessState,
        ])
        .values_panic([
            Expr::val(run.id.to_string()),
            Expr::val(run.workspace_id.to_string()),
            Expr::val(snapshot.tool_key().as_str()),
            Expr::val(snapshot.path().relative_path()),
            Expr::val(snapshot.source().as_str()),
            Expr::val(stored_revision(snapshot)?),
            Expr::val(snapshot.default_access_state().as_storage_str()),
        ])
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}

#[derive(Iden, Clone, Copy)]
pub(super) enum ProgramRunSharedToolSnapshots {
    Table,
    RunId,
    WorkspaceId,
    SharedToolId,
    Path,
    Revision,
    Source,
    DefaultAccessState,
}

/// Records the Shared Tool revision the Run linked, with the source it runs.
async fn insert_shared_run_snapshot(
    transaction: &mut Transaction<'_>,
    run: &AcceptedRun,
    snapshot: &LinkedToolSelection,
) -> Result<(), Error> {
    let revision = stored_revision(snapshot)?;
    let statement = Query::insert()
        .into_table(ProgramRunSharedToolSnapshots::Table)
        .columns([
            ProgramRunSharedToolSnapshots::RunId,
            ProgramRunSharedToolSnapshots::WorkspaceId,
            ProgramRunSharedToolSnapshots::SharedToolId,
            ProgramRunSharedToolSnapshots::Path,
            ProgramRunSharedToolSnapshots::Revision,
            ProgramRunSharedToolSnapshots::Source,
            ProgramRunSharedToolSnapshots::DefaultAccessState,
        ])
        .values_panic([
            Expr::val(run.id.to_string()),
            Expr::val(run.workspace_id.to_string()),
            Expr::val(snapshot.tool_key().as_str()),
            Expr::val(snapshot.path().relative_path()),
            Expr::val(revision),
            Expr::val(snapshot.source().as_str()),
            Expr::val(snapshot.default_access_state().as_storage_str()),
        ])
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}
