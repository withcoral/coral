//! Shared Tool rows: records with their current revision pointer,
//! immutable revisions, the Shared Tools each revision calls, and lifecycle
//! Events. Every read and write matches the Workspace and the entity.

use coral_db::{RowLock, Transaction};
use coral_workspace::{TraceId, UserId, WorkspaceId};
use sea_query::{Expr, ExprTrait as _, Iden, Order, Query, SelectStatement};
use sqlx::FromRow;

use crate::Error;
use crate::repository::{access_state_from_storage, decode_diagnostics, stored_revision};
use crate::shared_tool::{
    SharedTool, SharedToolDependency, SharedToolId, SharedToolLifecycleAction, SharedToolPage,
    SharedToolPath, SharedToolRevision, SharedToolSelection, SharedToolSummary,
};
use crate::types::{
    PrivateToolDeclaration, PrivateToolDeclarationFieldProjection, PrivateToolDescription,
    PrivateToolId, PrivateToolSource,
};

#[derive(Iden)]
enum SharedToolRecords {
    Table,
    Id,
    WorkspaceId,
    Path,
    CurrentRevision,
    CreatedByUserId,
    CreatedAtUnixNanos,
    RemovedAtUnixNanos,
}

#[derive(Iden)]
enum SharedToolRevisions {
    Table,
    SharedToolId,
    Revision,
    WorkspaceId,
    Description,
    Source,
    Declaration,
    DeclarationFieldsFormat,
    DeclarationFieldsStatus,
    DeclarationFieldsJson,
    DeclarationFieldsFailure,
    DiagnosticsJson,
    DiagnosticsTruncated,
    ContentHash,
    DefaultAccessState,
    PublishedByUserId,
    SourcePrivateToolId,
    RestoredFromRevision,
    PublishedAtUnixNanos,
}

#[derive(Iden)]
enum SharedToolRevisionDependencies {
    Table,
    SharedToolId,
    Revision,
    DependencySharedToolId,
    WorkspaceId,
    DependencyRevision,
}

#[derive(Iden)]
enum SharedToolLifecycleEvents {
    Table,
    Id,
    WorkspaceId,
    ActorUserId,
    AuthoritySource,
    Action,
    TargetSharedToolId,
    TargetPath,
    Revision,
    ContentHash,
    OccurredAtUnixNanos,
    TraceId,
}

/// One live Shared Tool record, without its revisions.
pub(crate) struct SharedToolRecord {
    pub(crate) id: SharedToolId,
    pub(crate) path: SharedToolPath,
    pub(crate) current_revision: u64,
    pub(crate) created_by: UserId,
    pub(crate) created_at_unix_nanos: i64,
}

#[derive(Debug, FromRow)]
struct RecordRow {
    id: String,
    path: String,
    current_revision: i64,
    created_by_user_id: String,
    created_at_unix_nanos: i64,
}

impl RecordRow {
    fn decode(self) -> Result<SharedToolRecord, Error> {
        Ok(SharedToolRecord {
            id: decode_id(&self.id)?,
            path: decode_path(&self.path)?,
            current_revision: decode_revision(self.current_revision)?,
            created_by: decode_user(&self.created_by_user_id)?,
            created_at_unix_nanos: self.created_at_unix_nanos,
        })
    }
}

#[derive(Debug, FromRow)]
struct RevisionRow {
    shared_tool_id: String,
    revision: i64,
    description: String,
    source: String,
    declaration: String,
    declaration_fields_format: String,
    declaration_fields_status: String,
    declaration_fields_json: Option<String>,
    declaration_fields_failure: Option<String>,
    diagnostics_json: String,
    diagnostics_truncated: bool,
    content_hash: String,
    default_access_state: String,
    published_by_user_id: String,
    source_private_tool_id: Option<String>,
    restored_from_revision: Option<i64>,
    published_at_unix_nanos: i64,
}

impl RevisionRow {
    fn decode(self) -> Result<SharedToolRevision, Error> {
        let corrupt = |what: &str| Error::CorruptData(format!("Shared Tool revision has {what}"));
        Ok(SharedToolRevision {
            shared_tool_id: decode_id(&self.shared_tool_id)?,
            revision: decode_revision(self.revision)?,
            description: PrivateToolDescription::parse(&self.description)
                .map_err(|_error| corrupt("an invalid description"))?,
            source: PrivateToolSource::parse(&self.source)
                .map_err(|_error| corrupt("an invalid source"))?,
            declaration: PrivateToolDeclaration::from_persisted(&self.declaration)?,
            declaration_field_projection: PrivateToolDeclarationFieldProjection::from_storage(
                &self.declaration_fields_format,
                &self.declaration_fields_status,
                self.declaration_fields_json.as_deref(),
                self.declaration_fields_failure.as_deref(),
            )?,
            diagnostics: decode_diagnostics(&self.diagnostics_json, self.diagnostics_truncated)?,
            diagnostics_truncated: self.diagnostics_truncated,
            content_hash: self.content_hash,
            default_access_state: access_state_from_storage(&self.default_access_state)?,
            published_by: decode_user(&self.published_by_user_id)?,
            source_private_tool_id: self
                .source_private_tool_id
                .as_deref()
                .map(PrivateToolId::parse)
                .transpose()
                .map_err(|_error| corrupt("an invalid source Private Tool"))?,
            restored_from_revision: self
                .restored_from_revision
                .map(decode_revision)
                .transpose()?,
            published_at_unix_nanos: self.published_at_unix_nanos,
        })
    }
}

fn decode_id(value: &str) -> Result<SharedToolId, Error> {
    SharedToolId::parse(value)
        .map_err(|_error| Error::CorruptData("Shared Tool has invalid identity".to_string()))
}

fn decode_path(value: &str) -> Result<SharedToolPath, Error> {
    SharedToolPath::parse(value)
        .map_err(|_error| Error::CorruptData("Shared Tool has invalid path".to_string()))
}

fn decode_user(value: &str) -> Result<UserId, Error> {
    UserId::parse(value)
        .map_err(|_error| Error::CorruptData("Shared Tool has invalid User identity".to_string()))
}

fn decode_revision(stored: i64) -> Result<u64, Error> {
    u64::try_from(stored)
        .ok()
        .filter(|revision| *revision > 0)
        .ok_or_else(|| Error::CorruptData("Shared Tool has invalid revision".to_string()))
}

fn record_columns() -> [SharedToolRecords; 5] {
    [
        SharedToolRecords::Id,
        SharedToolRecords::Path,
        SharedToolRecords::CurrentRevision,
        SharedToolRecords::CreatedByUserId,
        SharedToolRecords::CreatedAtUnixNanos,
    ]
}

fn revision_columns() -> [SharedToolRevisions; 17] {
    [
        SharedToolRevisions::SharedToolId,
        SharedToolRevisions::Revision,
        SharedToolRevisions::Description,
        SharedToolRevisions::Source,
        SharedToolRevisions::Declaration,
        SharedToolRevisions::DeclarationFieldsFormat,
        SharedToolRevisions::DeclarationFieldsStatus,
        SharedToolRevisions::DeclarationFieldsJson,
        SharedToolRevisions::DeclarationFieldsFailure,
        SharedToolRevisions::DiagnosticsJson,
        SharedToolRevisions::DiagnosticsTruncated,
        SharedToolRevisions::ContentHash,
        SharedToolRevisions::DefaultAccessState,
        SharedToolRevisions::PublishedByUserId,
        SharedToolRevisions::SourcePrivateToolId,
        SharedToolRevisions::RestoredFromRevision,
        SharedToolRevisions::PublishedAtUnixNanos,
    ]
}

/// The live record at `path` in the Workspace.
fn live_record_statement(workspace_id: &WorkspaceId, path: &SharedToolPath) -> SelectStatement {
    Query::select()
        .columns(record_columns())
        .from(SharedToolRecords::Table)
        .and_where(Expr::col(SharedToolRecords::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SharedToolRecords::Path).eq(path.as_str()))
        .and_where(Expr::col(SharedToolRecords::RemovedAtUnixNanos).is_null())
        .to_owned()
}

/// The live record at `path`. With `hold`, the row is held exclusively for
/// the write that follows, by a no-op `UPDATE`.
pub(crate) async fn find_live_record(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    path: &SharedToolPath,
    hold: bool,
) -> Result<Option<SharedToolRecord>, Error> {
    if hold {
        let statement = Query::update()
            .table(SharedToolRecords::Table)
            .value(
                SharedToolRecords::CurrentRevision,
                Expr::col(SharedToolRecords::CurrentRevision),
            )
            .and_where(Expr::col(SharedToolRecords::WorkspaceId).eq(workspace_id.to_string()))
            .and_where(Expr::col(SharedToolRecords::Path).eq(path.as_str()))
            .and_where(Expr::col(SharedToolRecords::RemovedAtUnixNanos).is_null())
            .to_owned();
        transaction.execute(statement).await?;
    }
    let row: Option<RecordRow> = transaction
        .fetch_optional(live_record_statement(workspace_id, path))
        .await?;
    row.map(RecordRow::decode).transpose()
}

/// The paths of every live Shared Tool in the Workspace.
pub(crate) async fn live_paths(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<Vec<SharedToolPath>, Error> {
    let statement = Query::select()
        .column(SharedToolRecords::Path)
        .from(SharedToolRecords::Table)
        .and_where(Expr::col(SharedToolRecords::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SharedToolRecords::RemovedAtUnixNanos).is_null())
        .to_owned();
    let rows: Vec<(String,)> = transaction.fetch_all(statement).await?;
    rows.iter().map(|(path,)| decode_path(path)).collect()
}

/// One revision of the Shared Tool `id` in the Workspace.
pub(crate) async fn find_revision(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    id: &SharedToolId,
    revision: u64,
) -> Result<Option<SharedToolRevision>, Error> {
    let statement = Query::select()
        .columns(revision_columns())
        .from(SharedToolRevisions::Table)
        .and_where(Expr::col(SharedToolRevisions::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SharedToolRevisions::SharedToolId).eq(id.to_string()))
        .and_where(Expr::col(SharedToolRevisions::Revision).eq(stored_revision(revision)?))
        .to_owned();
    let row: Option<RevisionRow> = transaction.fetch_optional(statement).await?;
    row.map(RevisionRow::decode).transpose()
}

/// Every revision of the Shared Tool `id` in the Workspace, newest first.
pub(crate) async fn list_revisions(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    id: &SharedToolId,
) -> Result<Vec<SharedToolRevision>, Error> {
    let statement = Query::select()
        .columns(revision_columns())
        .from(SharedToolRevisions::Table)
        .and_where(Expr::col(SharedToolRevisions::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SharedToolRevisions::SharedToolId).eq(id.to_string()))
        .order_by(SharedToolRevisions::Revision, Order::Desc)
        .to_owned();
    let rows: Vec<RevisionRow> = transaction.fetch_all(statement).await?;
    rows.into_iter().map(RevisionRow::decode).collect()
}

/// The live Shared Tool at `path` with its current revision.
pub(crate) async fn find_live_tool(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    path: &SharedToolPath,
) -> Result<Option<SharedTool>, Error> {
    let Some(record) = find_live_record(transaction, workspace_id, path, false).await? else {
        return Ok(None);
    };
    let current = find_revision(
        transaction,
        workspace_id,
        &record.id,
        record.current_revision,
    )
    .await?
    .ok_or_else(|| Error::CorruptData("Shared Tool lost its current revision".to_string()))?;
    Ok(Some(SharedTool {
        id: record.id,
        workspace_id: workspace_id.clone(),
        path: record.path,
        created_by: record.created_by,
        created_at_unix_nanos: record.created_at_unix_nanos,
        current,
    }))
}

#[derive(Debug, FromRow)]
struct SummaryRow {
    id: String,
    path: String,
    current_revision: i64,
    description: String,
    published_at_unix_nanos: i64,
}

/// One page of live Shared Tools in path order after `after`.
pub(crate) async fn list_live_page(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    after: Option<&SharedToolPath>,
    page_size: u32,
) -> Result<SharedToolPage, Error> {
    // Match Rust string ordering regardless of the Postgres locale, so the
    // exclusive cursor predicate agrees with the page order.
    let path = match transaction.system() {
        coral_db::DatabaseSystem::Sqlite => {
            Expr::col((SharedToolRecords::Table, SharedToolRecords::Path))
        }
        coral_db::DatabaseSystem::Postgres => {
            Expr::cust("\"shared_tool_records\".\"path\" COLLATE \"C\"")
        }
    };
    let mut statement = Query::select()
        .column((SharedToolRecords::Table, SharedToolRecords::Id))
        .column((SharedToolRecords::Table, SharedToolRecords::Path))
        .column((SharedToolRecords::Table, SharedToolRecords::CurrentRevision))
        .column((SharedToolRevisions::Table, SharedToolRevisions::Description))
        .column((
            SharedToolRevisions::Table,
            SharedToolRevisions::PublishedAtUnixNanos,
        ))
        .from(SharedToolRecords::Table)
        .inner_join(
            SharedToolRevisions::Table,
            Expr::col((
                SharedToolRevisions::Table,
                SharedToolRevisions::SharedToolId,
            ))
            .equals((SharedToolRecords::Table, SharedToolRecords::Id))
            .and(
                Expr::col((SharedToolRevisions::Table, SharedToolRevisions::Revision))
                    .equals((SharedToolRecords::Table, SharedToolRecords::CurrentRevision)),
            )
            .and(
                Expr::col((SharedToolRevisions::Table, SharedToolRevisions::WorkspaceId))
                    .equals((SharedToolRecords::Table, SharedToolRecords::WorkspaceId)),
            ),
        )
        .and_where(
            Expr::col((SharedToolRecords::Table, SharedToolRecords::WorkspaceId))
                .eq(workspace_id.to_string()),
        )
        .and_where(
            Expr::col((
                SharedToolRecords::Table,
                SharedToolRecords::RemovedAtUnixNanos,
            ))
            .is_null(),
        )
        .order_by_expr(path.clone(), Order::Asc)
        .limit(u64::from(page_size) + 1)
        .to_owned();
    if let Some(after) = after {
        statement.and_where(path.gt(after.as_str()));
    }
    let rows: Vec<SummaryRow> = transaction.fetch_all(statement).await?;
    let mut summaries = rows
        .into_iter()
        .map(|row| {
            Ok(SharedToolSummary {
                id: decode_id(&row.id)?,
                path: decode_path(&row.path)?,
                current_revision: decode_revision(row.current_revision)?,
                description: PrivateToolDescription::parse(&row.description).map_err(|_error| {
                    Error::CorruptData(
                        "Shared Tool revision has an invalid description".to_string(),
                    )
                })?,
                published_at_unix_nanos: row.published_at_unix_nanos,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let page_size = usize::try_from(page_size).unwrap_or(usize::MAX);
    let next_after = if summaries.len() > page_size {
        summaries.truncate(page_size);
        summaries.last().map(|summary| summary.path.clone())
    } else {
        None
    };
    Ok(SharedToolPage {
        summaries,
        next_after,
    })
}

#[derive(Debug, FromRow)]
struct SelectionRow {
    id: String,
    current_revision: i64,
    source: String,
    default_access_state: String,
}

/// The current revision of the live Shared Tool at `path`, for a Run to
/// link.
pub(crate) async fn select_current_source(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    path: &SharedToolPath,
) -> Result<Option<SharedToolSelection>, Error> {
    let statement = Query::select()
        .column((SharedToolRecords::Table, SharedToolRecords::Id))
        .column((SharedToolRecords::Table, SharedToolRecords::CurrentRevision))
        .column((SharedToolRevisions::Table, SharedToolRevisions::Source))
        .column((
            SharedToolRevisions::Table,
            SharedToolRevisions::DefaultAccessState,
        ))
        .from(SharedToolRecords::Table)
        .inner_join(
            SharedToolRevisions::Table,
            Expr::col((
                SharedToolRevisions::Table,
                SharedToolRevisions::SharedToolId,
            ))
            .equals((SharedToolRecords::Table, SharedToolRecords::Id))
            .and(
                Expr::col((SharedToolRevisions::Table, SharedToolRevisions::Revision))
                    .equals((SharedToolRecords::Table, SharedToolRecords::CurrentRevision)),
            )
            .and(
                Expr::col((SharedToolRevisions::Table, SharedToolRevisions::WorkspaceId))
                    .equals((SharedToolRecords::Table, SharedToolRecords::WorkspaceId)),
            ),
        )
        .and_where(
            Expr::col((SharedToolRecords::Table, SharedToolRecords::WorkspaceId))
                .eq(workspace_id.to_string()),
        )
        .and_where(Expr::col((SharedToolRecords::Table, SharedToolRecords::Path)).eq(path.as_str()))
        .and_where(
            Expr::col((
                SharedToolRecords::Table,
                SharedToolRecords::RemovedAtUnixNanos,
            ))
            .is_null(),
        )
        .to_owned();
    let row: Option<SelectionRow> = transaction.fetch_optional(statement).await?;
    row.map(|row| {
        Ok(SharedToolSelection {
            id: decode_id(&row.id)?,
            path: path.clone(),
            revision: decode_revision(row.current_revision)?,
            source: PrivateToolSource::parse(&row.source).map_err(|_error| {
                Error::CorruptData("Shared Tool revision has an invalid source".to_string())
            })?,
            default_access_state: access_state_from_storage(&row.default_access_state)?,
        })
    })
    .transpose()
}

/// Whether `selection` is still the live, current revision at its path,
/// holding the record as `lock` says.
pub(crate) async fn hold_current(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    selection: &SharedToolSelection,
    lock: RowLock,
) -> Result<bool, Error> {
    let statement = Query::select()
        .column(SharedToolRecords::Id)
        .from(SharedToolRecords::Table)
        .and_where(Expr::col(SharedToolRecords::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SharedToolRecords::Id).eq(selection.id().to_string()))
        .and_where(Expr::col(SharedToolRecords::Path).eq(selection.path().as_str()))
        .and_where(
            Expr::col(SharedToolRecords::CurrentRevision)
                .eq(stored_revision(selection.revision())?),
        )
        .and_where(Expr::col(SharedToolRecords::RemovedAtUnixNanos).is_null())
        .to_owned();
    let row: Option<(String,)> = transaction.fetch_optional_locked(statement, lock).await?;
    Ok(row.is_some())
}

/// Inserts a new Shared Tool's record at its first revision.
pub(crate) async fn insert_record(
    transaction: &mut Transaction<'_>,
    tool: &SharedTool,
) -> Result<(), Error> {
    let statement = Query::insert()
        .into_table(SharedToolRecords::Table)
        .columns([
            SharedToolRecords::Id,
            SharedToolRecords::WorkspaceId,
            SharedToolRecords::Path,
            SharedToolRecords::CurrentRevision,
            SharedToolRecords::CreatedByUserId,
            SharedToolRecords::CreatedAtUnixNanos,
        ])
        .values_panic([
            Expr::val(tool.id().to_string()),
            Expr::val(tool.workspace_id().to_string()),
            Expr::val(tool.path().as_str()),
            Expr::val(stored_revision(tool.current().revision())?),
            Expr::val(tool.created_by().as_str()),
            Expr::val(tool.created_at_unix_nanos()),
        ])
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}

/// Moves the live record `id` from revision `from` to `to`.
pub(crate) async fn advance_current(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    id: &SharedToolId,
    from: u64,
    to: u64,
) -> Result<(), Error> {
    let statement = Query::update()
        .table(SharedToolRecords::Table)
        .value(SharedToolRecords::CurrentRevision, stored_revision(to)?)
        .and_where(Expr::col(SharedToolRecords::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SharedToolRecords::Id).eq(id.to_string()))
        .and_where(Expr::col(SharedToolRecords::CurrentRevision).eq(stored_revision(from)?))
        .and_where(Expr::col(SharedToolRecords::RemovedAtUnixNanos).is_null())
        .to_owned();
    if transaction.execute(statement).await? != 1 {
        return Err(Error::CorruptData(
            "a held Shared Tool moved inside its transaction".to_string(),
        ));
    }
    Ok(())
}

/// Inserts one immutable revision.
pub(crate) async fn insert_revision(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    revision: &SharedToolRevision,
) -> Result<(), Error> {
    let projection = revision.declaration_field_projection();
    let statement = Query::insert()
        .into_table(SharedToolRevisions::Table)
        .columns([
            SharedToolRevisions::SharedToolId,
            SharedToolRevisions::Revision,
            SharedToolRevisions::WorkspaceId,
            SharedToolRevisions::Description,
            SharedToolRevisions::Source,
            SharedToolRevisions::Declaration,
            SharedToolRevisions::DeclarationFieldsFormat,
            SharedToolRevisions::DeclarationFieldsStatus,
            SharedToolRevisions::DeclarationFieldsJson,
            SharedToolRevisions::DeclarationFieldsFailure,
            SharedToolRevisions::DiagnosticsJson,
            SharedToolRevisions::DiagnosticsTruncated,
            SharedToolRevisions::ContentHash,
            SharedToolRevisions::DefaultAccessState,
            SharedToolRevisions::PublishedByUserId,
            SharedToolRevisions::SourcePrivateToolId,
            SharedToolRevisions::RestoredFromRevision,
            SharedToolRevisions::PublishedAtUnixNanos,
        ])
        .values_panic([
            Expr::val(revision.shared_tool_id().to_string()),
            Expr::val(stored_revision(revision.revision())?),
            Expr::val(workspace_id.to_string()),
            Expr::val(revision.description().as_str()),
            Expr::val(revision.source().as_str()),
            Expr::val(revision.declaration().as_str()),
            Expr::val(PrivateToolDeclarationFieldProjection::format()),
            Expr::val(projection.status()),
            Expr::val(projection.fields().map(serde_json::to_string).transpose()?),
            Expr::val(projection.failure_reason()),
            Expr::val(serde_json::to_string(revision.diagnostics())?),
            Expr::val(revision.diagnostics_truncated()),
            Expr::val(revision.content_hash()),
            Expr::val(revision.default_access_state().as_storage_str()),
            Expr::val(revision.published_by().as_str()),
            Expr::val(revision.source_private_tool_id().map(ToString::to_string)),
            Expr::val(
                revision
                    .restored_from_revision()
                    .map(stored_revision)
                    .transpose()?,
            ),
            Expr::val(revision.published_at_unix_nanos()),
        ])
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}

/// Records the Shared Tools one revision calls.
pub(crate) async fn insert_dependencies(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    revision: &SharedToolRevision,
    dependencies: &[SharedToolDependency],
) -> Result<(), Error> {
    for dependency in dependencies {
        let statement = Query::insert()
            .into_table(SharedToolRevisionDependencies::Table)
            .columns([
                SharedToolRevisionDependencies::SharedToolId,
                SharedToolRevisionDependencies::Revision,
                SharedToolRevisionDependencies::DependencySharedToolId,
                SharedToolRevisionDependencies::WorkspaceId,
                SharedToolRevisionDependencies::DependencyRevision,
            ])
            .values_panic([
                Expr::val(revision.shared_tool_id().to_string()),
                Expr::val(stored_revision(revision.revision())?),
                Expr::val(dependency.id().to_string()),
                Expr::val(workspace_id.to_string()),
                Expr::val(stored_revision(dependency.revision())?),
            ])
            .to_owned();
        transaction.execute(statement).await?;
    }
    Ok(())
}

pub(crate) struct NewLifecycleEvent<'a> {
    pub(crate) workspace_id: &'a WorkspaceId,
    pub(crate) actor: &'a UserId,
    pub(crate) action: SharedToolLifecycleAction,
    pub(crate) target_id: &'a SharedToolId,
    pub(crate) target_path: &'a SharedToolPath,
    pub(crate) revision: u64,
    pub(crate) content_hash: &'a str,
    pub(crate) occurred_at_unix_nanos: i64,
    pub(crate) trace_id: &'a TraceId,
}

/// Records one publish or removal. Workspace Membership is its only
/// authority source.
pub(crate) async fn insert_lifecycle_event(
    transaction: &mut Transaction<'_>,
    event: NewLifecycleEvent<'_>,
) -> Result<(), Error> {
    let statement = Query::insert()
        .into_table(SharedToolLifecycleEvents::Table)
        .columns([
            SharedToolLifecycleEvents::Id,
            SharedToolLifecycleEvents::WorkspaceId,
            SharedToolLifecycleEvents::ActorUserId,
            SharedToolLifecycleEvents::AuthoritySource,
            SharedToolLifecycleEvents::Action,
            SharedToolLifecycleEvents::TargetSharedToolId,
            SharedToolLifecycleEvents::TargetPath,
            SharedToolLifecycleEvents::Revision,
            SharedToolLifecycleEvents::ContentHash,
            SharedToolLifecycleEvents::OccurredAtUnixNanos,
            SharedToolLifecycleEvents::TraceId,
        ])
        .values_panic([
            Expr::val(uuid::Uuid::new_v4().hyphenated().to_string()),
            Expr::val(event.workspace_id.to_string()),
            Expr::val(event.actor.as_str()),
            Expr::val(coral_workspace::AuthoritySource::WorkspaceMembership.as_storage_str()),
            Expr::val(event.action.as_storage_str()),
            Expr::val(event.target_id.to_string()),
            Expr::val(event.target_path.as_str()),
            Expr::val(stored_revision(event.revision)?),
            Expr::val(event.content_hash),
            Expr::val(event.occurred_at_unix_nanos),
            Expr::val(event.trace_id.as_str()),
        ])
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}
