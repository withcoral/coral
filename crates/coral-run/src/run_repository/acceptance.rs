//! The accepted Program package, its linked source snapshots, and `run.started`.

use coral_db::Transaction;
use coral_functions::SavedFunctionSelection;
use coral_workspace::WorkspaceId;
use sea_query::{Expr, ExprTrait as _, Iden, OnConflict, Query};
use sqlx::FromRow;

use super::events::{NewEvent, insert_events};
use super::{ChunkEvent, ProgramRunRecords};
use crate::CoralInstanceId;
use crate::engine::ProgramPackage;
use crate::types::AcceptedRun;
use crate::{
    COMPILATION_TIMEOUT, EMITTED_JAVASCRIPT_MAX_BYTES, Error, PROGRAM_SOURCE_MAX_BYTES,
    ProgramSource, RunId, RunState, SOURCE_MAP_MAX_BYTES,
};

#[cfg(test)]
mod tests;

#[derive(Iden)]
enum ProgramRunPackages {
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

#[derive(Debug, FromRow)]
struct PackageRow {
    emitted_javascript: String,
    source_map: String,
    compiler_diagnostics_json: String,
    typescript_profile_id: String,
    compiler_version: String,
    javascript_target: String,
}

impl PackageRow {
    fn decode(self) -> Result<ProgramPackage, Error> {
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

/// Inserts the Run row, claimed by `holder`, and its package. `false` means a
/// Run with this identifier already exists and nothing was written: the
/// caller draws a new identifier.
pub(crate) async fn insert_acceptance(
    transaction: &mut Transaction<'_>,
    run: &AcceptedRun,
    holder: &CoralInstanceId,
    source: &ProgramSource,
    package: &ProgramPackage,
    linked: &[SavedFunctionSelection],
) -> Result<bool, Error> {
    let program_source_max_bytes = stored_usize(PROGRAM_SOURCE_MAX_BYTES)?;
    let emitted_javascript_max_bytes = stored_usize(EMITTED_JAVASCRIPT_MAX_BYTES)?;
    let source_map_max_bytes = stored_usize(SOURCE_MAP_MAX_BYTES)?;
    let program_value_max_bytes = stored_usize(run.value_limits.host_bytes())?;
    let compilation_timeout_millis =
        i64::try_from(COMPILATION_TIMEOUT.as_millis()).map_err(|_error| {
            Error::CorruptData("compilation timeout exceeds durable range".to_string())
        })?;
    if !insert_record(transaction, run, holder).await? {
        return Ok(false);
    }

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
        insert_run_snapshot(transaction, run, snapshot).await?;
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

/// Inserts the Run row. `false` means a Run with this identifier already
/// exists and nothing was written. The conflict target is the primary key, so
/// a Postgres transaction stays usable after it.
async fn insert_record(
    transaction: &mut Transaction<'_>,
    run: &AcceptedRun,
    holder: &CoralInstanceId,
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
            ProgramRunRecords::DeadlineAtUnixNanos,
            ProgramRunRecords::ShutdownGraceNanos,
            ProgramRunRecords::WorkerClaimId,
            ProgramRunRecords::WorkerClaimExpiresAtUnixNanos,
            ProgramRunRecords::HolderInstanceId,
            ProgramRunRecords::ValueLimitsJson,
            ProgramRunRecords::NextEventSequence,
            ProgramRunRecords::AccumulatedOutputBytes,
            ProgramRunRecords::ChunkSequence,
        ])
        .values_panic([
            Expr::val(run.id.to_string()),
            Expr::val(run.workspace_id.to_string()),
            Expr::val(run.principal.id().as_str()),
            Expr::val(run.principal.kind().as_storage_str()),
            Expr::val(RunState::Running.as_storage_str()),
            Expr::val(run.accepted_at_unix_nanos),
            Expr::val(run.deadline_at_unix_nanos),
            Expr::val(run.shutdown_grace_nanos),
            // The Run Owner's worker claim: written with acceptance, so no
            // running Run is ever unclaimed, and renewed by every chunk.
            Expr::val(run.claim_id.as_str()),
            Expr::val(run.worker_claim_expires_at_unix_nanos),
            Expr::val(holder.as_str()),
            Expr::val(serde_json::to_string(&run.value_limits).map_err(Error::ProgramValueJson)?),
            // The previous release's Event counter: this release advances it per
            // chunk for that release's writers and never reads it.
            Expr::val(2_i64),
            Expr::val(0_i64),
            Expr::val(0_i64),
        ])
        .on_conflict(
            OnConflict::column(ProgramRunRecords::Id)
                .do_nothing()
                .to_owned(),
        )
        .to_owned();
    Ok(transaction.execute(statement).await? == 1)
}

pub(crate) async fn load_package_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
) -> Result<Option<ProgramPackage>, Error> {
    let statement = Query::select()
        .columns([
            ProgramRunPackages::EmittedJavascript,
            ProgramRunPackages::SourceMap,
            ProgramRunPackages::CompilerDiagnosticsJson,
            ProgramRunPackages::TypescriptProfileId,
            ProgramRunPackages::CompilerVersion,
            ProgramRunPackages::JavascriptTarget,
        ])
        .from(ProgramRunPackages::Table)
        .and_where(Expr::col(ProgramRunPackages::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(ProgramRunPackages::RunId).eq(run_id.to_string()))
        .to_owned();
    let row: Option<PackageRow> = transaction.fetch_optional(statement).await?;
    row.map(PackageRow::decode).transpose()
}

fn stored_usize(value: usize) -> Result<i64, Error> {
    i64::try_from(value)
        .map_err(|_error| Error::CorruptData("Program limit exceeds durable range".to_string()))
}

#[derive(Iden)]
enum ProgramRunSavedFunctionSnapshots {
    Table,
    RunId,
    WorkspaceId,
    SavedFunctionId,
    Path,
    Source,
}

async fn insert_run_snapshot(
    transaction: &mut Transaction<'_>,
    run: &AcceptedRun,
    snapshot: &SavedFunctionSelection,
) -> Result<(), Error> {
    let statement = Query::insert()
        .into_table(ProgramRunSavedFunctionSnapshots::Table)
        .columns([
            ProgramRunSavedFunctionSnapshots::RunId,
            ProgramRunSavedFunctionSnapshots::WorkspaceId,
            ProgramRunSavedFunctionSnapshots::SavedFunctionId,
            ProgramRunSavedFunctionSnapshots::Path,
            ProgramRunSavedFunctionSnapshots::Source,
        ])
        .values_panic([
            Expr::val(run.id.to_string()),
            Expr::val(run.workspace_id.to_string()),
            Expr::val(snapshot.id().to_string()),
            Expr::val(snapshot.path().as_str()),
            Expr::val(snapshot.source().as_str()),
        ])
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}
