//! What a Run stream's loop reads and writes on its own: the Run its `Open`
//! names, the Tools the Run linked, the start chunk, and the chunk that
//! reopens a started Run's stream.

use std::collections::HashMap;

use coral_catalog::{AuthoredToolKind, CodeExecutionBinding, ToolAccessState};
use coral_db::{Database, Transaction};
use coral_workspace::WorkspaceId;
use sea_query::{Alias, Expr, ExprTrait as _, Query, UnionType};
use sqlx::FromRow;

use super::acceptance::{
    PACKAGE_COLUMNS, PackageRow, ProgramRunPackages, ProgramRunPrivateToolSnapshots,
    ProgramRunSharedToolSnapshots,
};
use super::events::OwnedEvent;
use super::ledger::{Chunk, commit_chunk_with};
use super::{ProgramRunRecords, RUN_COLUMNS, RunRow};
use crate::engine::ProgramPackage;
use crate::lifecycle::RunRecord;
use crate::types::LinkedTool;
use crate::{CoralInstanceId, Error, RunId};

#[derive(FromRow)]
struct TakenRow {
    #[sqlx(flatten)]
    run: RunRow,
    #[sqlx(flatten)]
    package: PackageRow,
}

/// The Run a Run stream's `Open` names, by its id and the execution claim a
/// take gave it, with its package, in one statement. The claim stands in
/// for the Workspace: only the Run Executor that took the Run holds it, and
/// every later read is scoped by the Workspace of the row found. `None`
/// when no Run holds that claim.
pub(crate) async fn find_taken(
    database: &Database,
    run_id: &RunId,
    claim_id: &str,
) -> Result<Option<(RunRecord, ProgramPackage)>, Error> {
    let runs = ProgramRunRecords::Table;
    let packages = ProgramRunPackages::Table;
    let row: Option<TakenRow> = database
        .fetch_optional(
            Query::select()
                .columns(RUN_COLUMNS.map(|column| (runs, column)))
                .columns(PACKAGE_COLUMNS.map(|column| (packages, column)))
                .from(runs)
                .inner_join(
                    packages,
                    Expr::col((packages, ProgramRunPackages::WorkspaceId))
                        .equals((runs, ProgramRunRecords::WorkspaceId))
                        .and(
                            Expr::col((packages, ProgramRunPackages::RunId))
                                .equals((runs, ProgramRunRecords::Id)),
                        ),
                )
                .and_where(Expr::col((runs, ProgramRunRecords::Id)).eq(run_id.to_string()))
                .and_where(Expr::col((runs, ProgramRunRecords::ExecutionClaimId)).eq(claim_id))
                .to_owned(),
        )
        .await?;
    row.map(|row| Ok((row.run.decode()?, row.package.decode()?)))
        .transpose()
}

/// One linked Tool as its snapshot row keeps it.
#[derive(FromRow)]
struct SnapshotRow {
    root: String,
    path: String,
    tool_id: String,
    revision: Option<i64>,
    default_access_state: Option<String>,
}

/// Each Private and Shared Tool `run` linked, by its complete Call Path, as
/// acceptance retained it, in one statement.
pub(crate) async fn linked_tools(
    database: &Database,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
) -> Result<HashMap<String, LinkedTool>, Error> {
    use ProgramRunPrivateToolSnapshots as Private;
    use ProgramRunSharedToolSnapshots as Shared;
    let mut private = Query::select()
        .expr_as(
            Expr::val(AuthoredToolKind::Private.call_root()),
            Alias::new("root"),
        )
        .column(Private::Path)
        .expr_as(Expr::col(Private::PrivateToolId), Alias::new("tool_id"))
        .columns([Private::Revision, Private::DefaultAccessState])
        .from(Private::Table)
        .and_where(Expr::col(Private::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(Private::RunId).eq(run_id.to_string()))
        .to_owned();
    let shared = Query::select()
        .expr_as(
            Expr::val(AuthoredToolKind::Shared.call_root()),
            Alias::new("root"),
        )
        .column(Shared::Path)
        .expr_as(Expr::col(Shared::SharedToolId), Alias::new("tool_id"))
        .columns([Shared::Revision, Shared::DefaultAccessState])
        .from(Shared::Table)
        .and_where(Expr::col(Shared::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(Shared::RunId).eq(run_id.to_string()))
        .to_owned();
    let rows: Vec<SnapshotRow> = database
        .fetch_all(private.union(UnionType::All, shared).to_owned())
        .await?;
    let mut linked = HashMap::new();
    for row in rows {
        let invalid = || Error::CorruptData("Run has an invalid linked Tool".to_string());
        let kind = AuthoredToolKind::ALL
            .into_iter()
            .find(|kind| kind.call_root() == row.root)
            .ok_or_else(invalid)?;
        let revision = row
            .revision
            .and_then(|revision| u64::try_from(revision).ok())
            .ok_or_else(invalid)?;
        let declared_default = row
            .default_access_state
            .as_deref()
            .and_then(ToolAccessState::from_storage_str)
            .ok_or_else(invalid)?;
        linked.insert(
            kind.call_path(&row.path),
            LinkedTool {
                kind,
                binding: CodeExecutionBinding::new(row.tool_id, revision),
                declared_default,
            },
        );
    }
    Ok(linked)
}

/// The start chunk of a taken Run: the start mark, the deadline, and this
/// Coral Instance as the stream's holder, with no request committed yet,
/// predicated on the take's claim and the head.
pub(crate) async fn start_taken(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    claim_id: &str,
    started_at_unix_nanos: i64,
    deadline_at_unix_nanos: i64,
    holder: &CoralInstanceId,
) -> Result<(), Error> {
    commit_chunk_with(
        transaction,
        Chunk::new(run, Some(claim_id), started_at_unix_nanos).request_number(0),
        vec![
            (
                ProgramRunRecords::ExecutionStartedAtUnixNanos,
                Expr::val(started_at_unix_nanos),
            ),
            (
                ProgramRunRecords::DeadlineAtUnixNanos,
                Expr::val(deadline_at_unix_nanos),
            ),
            (
                ProgramRunRecords::HolderInstanceId,
                Expr::val(holder.as_str()),
            ),
        ],
    )
    .await
}

/// The reopen of a started Run's stream on this Coral Instance: it holds
/// the stream from this chunk on, which carries the `invocation.ended`
/// Events of the calls in flight on the one before, predicated on the
/// take's claim and the head.
pub(crate) async fn reopen_taken(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    claim_id: &str,
    events: &[OwnedEvent],
    committed_at_unix_nanos: i64,
    holder: &CoralInstanceId,
) -> Result<(), Error> {
    commit_chunk_with(
        transaction,
        Chunk::new(run, Some(claim_id), committed_at_unix_nanos).owned_events(events),
        vec![(
            ProgramRunRecords::HolderInstanceId,
            Expr::val(holder.as_str()),
        )],
    )
    .await
}
