//! Shared Tool rows: records with their current revision pointer,
//! immutable revisions, the Shared Tools each revision calls, and lifecycle
//! Events. Every read and write matches the Workspace and the entity.

use std::collections::BTreeMap;

use coral_catalog::ToolAccessState;
use coral_db::{Page, PageRequest, RowLock, Transaction, text_key_order};
use coral_workspace::{TraceId, UserId, WorkspaceId};
use sea_query::{Expr, ExprTrait as _, Iden, Order, Query, SelectStatement};
use sqlx::FromRow;

use crate::Error;
use crate::repository::{access_state_from_storage, decode_diagnostics, stored_revision};
use crate::shared_tool::{
    SharedTool, SharedToolDefault, SharedToolDependency, SharedToolId, SharedToolLifecycleAction,
    SharedToolLifecycleEvent, SharedToolLifecycleEventId, SharedToolLifecycleEventKey,
    SharedToolPath, SharedToolRevision, SharedToolRevisionOrigin, SharedToolRevisionSummary,
    SharedToolSummary,
};
use crate::types::{
    AuthoredToolDeclaration, AuthoredToolDeclarationFieldProjection, AuthoredToolDescription,
    AuthoredToolSource, PrivateToolId,
};

#[derive(Iden)]
pub(crate) enum SharedToolRecords {
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
pub(crate) enum SharedToolRevisions {
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

/// One revision without its source, declaration, declaration fields or
/// diagnostics.
#[derive(Debug, FromRow)]
struct RevisionSummaryRow {
    revision: i64,
    description: String,
    content_hash: String,
    default_access_state: String,
    published_by_user_id: String,
    source_private_tool_id: Option<String>,
    restored_from_revision: Option<i64>,
    published_at_unix_nanos: i64,
}

impl RevisionSummaryRow {
    fn decode(self) -> Result<SharedToolRevisionSummary, Error> {
        Ok(SharedToolRevisionSummary {
            revision: decode_revision(self.revision)?,
            description: AuthoredToolDescription::parse(&self.description).map_err(|_error| {
                Error::CorruptData("Shared Tool revision has an invalid description".to_string())
            })?,
            content_hash: self.content_hash,
            default_access_state: access_state_from_storage(&self.default_access_state)?,
            published_by: decode_user(&self.published_by_user_id)?,
            origin: decode_origin(self.source_private_tool_id, self.restored_from_revision)?,
            published_at_unix_nanos: self.published_at_unix_nanos,
        })
    }
}

/// One whole revision: its summary, which decodes as every summary does,
/// and its definition.
#[derive(Debug, FromRow)]
struct RevisionRow {
    shared_tool_id: String,
    #[sqlx(flatten)]
    summary: RevisionSummaryRow,
    source: String,
    declaration: String,
    declaration_fields_format: String,
    declaration_fields_status: String,
    declaration_fields_json: Option<String>,
    declaration_fields_failure: Option<String>,
    diagnostics_json: String,
    diagnostics_truncated: bool,
}

impl RevisionRow {
    fn decode(self) -> Result<SharedToolRevision, Error> {
        let summary = self.summary.decode()?;
        Ok(SharedToolRevision {
            shared_tool_id: decode_id(&self.shared_tool_id)?,
            revision: summary.revision,
            description: summary.description,
            source: AuthoredToolSource::parse(&self.source).map_err(|_error| {
                Error::CorruptData("Shared Tool revision has an invalid source".to_string())
            })?,
            declaration: AuthoredToolDeclaration::from_persisted(&self.declaration)?,
            declaration_field_projection: AuthoredToolDeclarationFieldProjection::from_storage(
                &self.declaration_fields_format,
                &self.declaration_fields_status,
                self.declaration_fields_json.as_deref(),
                self.declaration_fields_failure.as_deref(),
            )?,
            diagnostics: decode_diagnostics(&self.diagnostics_json, self.diagnostics_truncated)?,
            diagnostics_truncated: self.diagnostics_truncated,
            content_hash: summary.content_hash,
            default_access_state: summary.default_access_state,
            published_by: summary.published_by,
            origin: summary.origin,
            published_at_unix_nanos: summary.published_at_unix_nanos,
        })
    }
}

/// What a revision copied: a Private Tool, or an earlier revision.
fn decode_origin(
    source_private_tool_id: Option<String>,
    restored_from_revision: Option<i64>,
) -> Result<SharedToolRevisionOrigin, Error> {
    let corrupt = |what: &str| Error::CorruptData(format!("Shared Tool revision has {what}"));
    match (source_private_tool_id, restored_from_revision) {
        (Some(private_tool), None) => Ok(SharedToolRevisionOrigin::PrivateTool(
            PrivateToolId::parse(&private_tool)
                .map_err(|_error| corrupt("an invalid source Private Tool"))?,
        )),
        (None, Some(revision)) => Ok(SharedToolRevisionOrigin::Revision(decode_revision(
            revision,
        )?)),
        _ => Err(corrupt("no one origin")),
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

/// The columns of a [`RevisionSummaryRow`].
const REVISION_SUMMARY_COLUMNS: [SharedToolRevisions; 8] = [
    SharedToolRevisions::Revision,
    SharedToolRevisions::Description,
    SharedToolRevisions::ContentHash,
    SharedToolRevisions::DefaultAccessState,
    SharedToolRevisions::PublishedByUserId,
    SharedToolRevisions::SourcePrivateToolId,
    SharedToolRevisions::RestoredFromRevision,
    SharedToolRevisions::PublishedAtUnixNanos,
];

/// The columns of a [`RevisionRow`]: its summary's and its definition's.
fn revision_columns() -> impl Iterator<Item = SharedToolRevisions> {
    REVISION_SUMMARY_COLUMNS.into_iter().chain([
        SharedToolRevisions::SharedToolId,
        SharedToolRevisions::Source,
        SharedToolRevisions::Declaration,
        SharedToolRevisions::DeclarationFieldsFormat,
        SharedToolRevisions::DeclarationFieldsStatus,
        SharedToolRevisions::DeclarationFieldsJson,
        SharedToolRevisions::DeclarationFieldsFailure,
        SharedToolRevisions::DiagnosticsJson,
        SharedToolRevisions::DiagnosticsTruncated,
    ])
}

/// Joins a record to its current revision, in the record's own Workspace.
pub(crate) fn is_current_revision() -> sea_query::SimpleExpr {
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
    )
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

/// The live record at `path`.
pub(crate) async fn find_live_record(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    path: &SharedToolPath,
) -> Result<Option<SharedToolRecord>, Error> {
    let row: Option<RecordRow> = transaction
        .fetch_optional(live_record_statement(workspace_id, path))
        .await?;
    row.map(RecordRow::decode).transpose()
}

/// The live record at `path`, held exclusively for the write that follows.
pub(crate) async fn hold_live_record(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    path: &SharedToolPath,
) -> Result<Option<SharedToolRecord>, Error> {
    let row: Option<RecordRow> = transaction
        .fetch_optional_locked(
            live_record_statement(workspace_id, path),
            RowLock::Exclusive,
        )
        .await?;
    row.map(RecordRow::decode).transpose()
}

/// Every live Shared Tool of the Workspace that has its current revision,
/// joined to that revision: the Shared Tools a Run links. A read selects
/// its columns from the record and the revision, and narrows the paths.
fn linkable_statement(workspace_id: &WorkspaceId) -> SelectStatement {
    Query::select()
        .from(SharedToolRecords::Table)
        .inner_join(SharedToolRevisions::Table, is_current_revision())
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
        .to_owned()
}

/// The Shared Tool a Run links at `path`, of [`linkable_statement`].
fn linkable_at_statement(workspace_id: &WorkspaceId, path: &SharedToolPath) -> SelectStatement {
    let mut statement = linkable_statement(workspace_id);
    statement.and_where(
        Expr::col((SharedToolRecords::Table, SharedToolRecords::Path)).eq(path.as_str()),
    );
    statement
}

/// The identity of the live Shared Tool at `path` that has its current
/// revision: the Shared Tool a Run links there. It reads no column of the
/// revision.
pub(crate) async fn find_live_id(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    path: &SharedToolPath,
) -> Result<Option<SharedToolId>, Error> {
    let mut statement = linkable_at_statement(workspace_id, path);
    statement.column((SharedToolRecords::Table, SharedToolRecords::Id));
    let row: Option<(String,)> = transaction.fetch_optional(statement).await?;
    row.map(|(id,)| decode_id(&id)).transpose()
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

/// The content hash of one revision of the Shared Tool `id` in the
/// Workspace.
pub(crate) async fn find_revision_content_hash(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    id: &SharedToolId,
    revision: u64,
) -> Result<Option<String>, Error> {
    let statement = Query::select()
        .column(SharedToolRevisions::ContentHash)
        .from(SharedToolRevisions::Table)
        .and_where(Expr::col(SharedToolRevisions::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SharedToolRevisions::SharedToolId).eq(id.to_string()))
        .and_where(Expr::col(SharedToolRevisions::Revision).eq(stored_revision(revision)?))
        .to_owned();
    let row: Option<(String,)> = transaction.fetch_optional(statement).await?;
    Ok(row.map(|(content_hash,)| content_hash))
}

/// Every revision of the Shared Tool `id` in the Workspace, newest first,
/// without its source, declaration, declaration fields or diagnostics.
pub(crate) async fn list_revision_summaries(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    id: &SharedToolId,
) -> Result<Vec<SharedToolRevisionSummary>, Error> {
    let statement = Query::select()
        .columns(REVISION_SUMMARY_COLUMNS)
        .from(SharedToolRevisions::Table)
        .and_where(Expr::col(SharedToolRevisions::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SharedToolRevisions::SharedToolId).eq(id.to_string()))
        .order_by(SharedToolRevisions::Revision, Order::Desc)
        .to_owned();
    let rows: Vec<RevisionSummaryRow> = transaction.fetch_all(statement).await?;
    rows.into_iter().map(RevisionSummaryRow::decode).collect()
}

/// The current declaration of each live Shared Tool at `paths` in the
/// Workspace. A path that names none is left out.
pub(crate) async fn live_declarations(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    paths: &[&SharedToolPath],
) -> Result<BTreeMap<SharedToolPath, AuthoredToolDeclaration>, Error> {
    if paths.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut statement = linkable_statement(workspace_id);
    statement
        .column((SharedToolRecords::Table, SharedToolRecords::Path))
        .column((SharedToolRevisions::Table, SharedToolRevisions::Declaration))
        .and_where(
            Expr::col((SharedToolRecords::Table, SharedToolRecords::Path))
                .is_in(paths.iter().map(|path| path.as_str())),
        );
    let rows: Vec<(String, String)> = transaction.fetch_all(statement).await?;
    rows.into_iter()
        .map(|(path, declaration)| {
            Ok((
                decode_path(&path)?,
                AuthoredToolDeclaration::from_persisted(&declaration)?,
            ))
        })
        .collect()
}

/// The live Shared Tool at `path` with its current revision.
pub(crate) async fn find_live_tool(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    path: &SharedToolPath,
) -> Result<Option<SharedTool>, Error> {
    let Some(record) = find_live_record(transaction, workspace_id, path).await? else {
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

/// One page of the Workspace's live Shared Tools in path order.
pub(crate) async fn list_live_page(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    request: &PageRequest<SharedToolPath>,
) -> Result<Page<SharedToolSummary, SharedToolPath>, Error> {
    let path = text_key_order(
        transaction.system(),
        (SharedToolRecords::Table, SharedToolRecords::Path),
    );
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
        .inner_join(SharedToolRevisions::Table, is_current_revision())
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
        .limit(request.size.lookahead_limit())
        .to_owned();
    if let Some(after) = &request.after {
        statement.and_where(path.gt(after.as_str()));
    }
    let rows: Vec<SummaryRow> = transaction.fetch_all(statement).await?;
    let summaries = rows
        .into_iter()
        .map(|row| {
            Ok(SharedToolSummary {
                id: decode_id(&row.id)?,
                path: decode_path(&row.path)?,
                current_revision: decode_revision(row.current_revision)?,
                description: AuthoredToolDescription::parse(&row.description).map_err(
                    |_error| {
                        Error::CorruptData(
                            "Shared Tool revision has an invalid description".to_string(),
                        )
                    },
                )?,
                published_at_unix_nanos: row.published_at_unix_nanos,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    Ok(Page::from_lookahead(summaries, request.size, |summary| {
        summary.path.clone()
    }))
}

/// Every live Shared Tool of the Workspace with the default of its current
/// revision: its id, its path and that default.
fn live_defaults_statement(workspace_id: &WorkspaceId) -> SelectStatement {
    Query::select()
        .column((SharedToolRecords::Table, SharedToolRecords::Id))
        .column((SharedToolRecords::Table, SharedToolRecords::Path))
        .column((
            SharedToolRevisions::Table,
            SharedToolRevisions::DefaultAccessState,
        ))
        .from(SharedToolRecords::Table)
        .inner_join(SharedToolRevisions::Table, is_current_revision())
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
        .to_owned()
}

fn decode_default((id, path, state): (String, String, String)) -> Result<SharedToolDefault, Error> {
    Ok(SharedToolDefault {
        key: decode_id(&id)?.tool_key(),
        path: decode_path(&path)?,
        default_access_state: access_state_from_storage(&state)?,
    })
}

/// One page of the Workspace's live Shared Tools with their defaults, in
/// Tool Key order after `after`, with one row more than `page_size` when
/// another page follows.
pub(crate) async fn live_default_page(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    after: Option<&coral_catalog::ToolKey>,
    page_size: u32,
) -> Result<Vec<SharedToolDefault>, Error> {
    // Match Rust string ordering regardless of the Postgres locale, so the
    // exclusive cursor predicate agrees with the page order.
    let id = match transaction.system() {
        coral_db::DatabaseSystem::Sqlite => {
            Expr::col((SharedToolRecords::Table, SharedToolRecords::Id))
        }
        coral_db::DatabaseSystem::Postgres => {
            Expr::cust("\"shared_tool_records\".\"id\" COLLATE \"C\"")
        }
    };
    let mut statement = live_defaults_statement(workspace_id);
    if let Some(after) = after {
        statement.and_where(id.clone().gt(after.as_str()));
    }
    statement
        .order_by_expr(id, Order::Asc)
        .limit(u64::from(page_size) + 1);
    let rows: Vec<(String, String, String)> = transaction.fetch_all(statement).await?;
    rows.into_iter().map(decode_default).collect()
}

/// The live Shared Tools of the Workspace that `tools` name, with their
/// defaults.
pub(crate) async fn live_defaults(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    tools: &[coral_catalog::ToolKey],
) -> Result<Vec<SharedToolDefault>, Error> {
    let mut statement = live_defaults_statement(workspace_id);
    statement.and_where(
        Expr::col((SharedToolRecords::Table, SharedToolRecords::Id))
            .is_in(tools.iter().map(|key| key.as_str().to_owned())),
    );
    let rows: Vec<(String, String, String)> = transaction.fetch_all(statement).await?;
    rows.into_iter().map(decode_default).collect()
}

/// The current revision of one live Shared Tool, as a Run links it and as
/// a publish records a dependency on it.
pub(crate) struct CurrentRevision {
    pub(crate) id: SharedToolId,
    pub(crate) path: SharedToolPath,
    pub(crate) revision: u64,
    pub(crate) source: AuthoredToolSource,
    pub(crate) default_access_state: ToolAccessState,
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
) -> Result<Option<CurrentRevision>, Error> {
    let mut statement = linkable_at_statement(workspace_id, path);
    statement
        .column((SharedToolRecords::Table, SharedToolRecords::Id))
        .column((SharedToolRecords::Table, SharedToolRecords::CurrentRevision))
        .column((SharedToolRevisions::Table, SharedToolRevisions::Source))
        .column((
            SharedToolRevisions::Table,
            SharedToolRevisions::DefaultAccessState,
        ));
    let row: Option<SelectionRow> = transaction.fetch_optional(statement).await?;
    row.map(|row| {
        Ok(CurrentRevision {
            id: decode_id(&row.id)?,
            path: path.clone(),
            revision: decode_revision(row.current_revision)?,
            source: AuthoredToolSource::parse(&row.source).map_err(|_error| {
                Error::CorruptData("Shared Tool revision has an invalid source".to_string())
            })?,
            default_access_state: access_state_from_storage(&row.default_access_state)?,
        })
    })
    .transpose()
}

/// Whether `revision` is still the current revision of the live Shared
/// Tool `key` at `path`, holding the record shared for the rest of the
/// transaction: concurrent readers hold it side by side, and a publish
/// waits for them.
pub(crate) async fn hold_current(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    key: &coral_catalog::ToolKey,
    path: &SharedToolPath,
    revision: u64,
) -> Result<bool, Error> {
    let statement = Query::select()
        .column(SharedToolRecords::Id)
        .from(SharedToolRecords::Table)
        .and_where(Expr::col(SharedToolRecords::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SharedToolRecords::Id).eq(key.as_str()))
        .and_where(Expr::col(SharedToolRecords::Path).eq(path.as_str()))
        .and_where(Expr::col(SharedToolRecords::CurrentRevision).eq(stored_revision(revision)?))
        .and_where(Expr::col(SharedToolRecords::RemovedAtUnixNanos).is_null())
        .to_owned();
    let row: Option<(String,)> = transaction
        .fetch_optional_locked(statement, RowLock::Shared)
        .await?;
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
    transaction.execute_change(statement).await?;
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
    if transaction.execute_change(statement).await? != 1 {
        return Err(Error::CorruptData(
            "a held Shared Tool moved inside its transaction".to_string(),
        ));
    }
    Ok(())
}

/// The path of a live Shared Tool whose current revision calls the Shared
/// Tool `id`: of several, the first in path order.
pub(crate) async fn first_dependent(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    id: &SharedToolId,
) -> Result<Option<SharedToolPath>, Error> {
    let calls_at_its_current_revision = Expr::col((
        SharedToolRevisionDependencies::Table,
        SharedToolRevisionDependencies::SharedToolId,
    ))
    .equals((SharedToolRecords::Table, SharedToolRecords::Id))
    .and(
        Expr::col((
            SharedToolRevisionDependencies::Table,
            SharedToolRevisionDependencies::Revision,
        ))
        .equals((SharedToolRecords::Table, SharedToolRecords::CurrentRevision)),
    )
    .and(
        Expr::col((
            SharedToolRevisionDependencies::Table,
            SharedToolRevisionDependencies::WorkspaceId,
        ))
        .equals((SharedToolRecords::Table, SharedToolRecords::WorkspaceId)),
    );
    let statement = Query::select()
        .column((SharedToolRecords::Table, SharedToolRecords::Path))
        .from(SharedToolRecords::Table)
        .inner_join(
            SharedToolRevisionDependencies::Table,
            calls_at_its_current_revision,
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
        .and_where(
            Expr::col((
                SharedToolRevisionDependencies::Table,
                SharedToolRevisionDependencies::DependencySharedToolId,
            ))
            .eq(id.to_string()),
        )
        .to_owned();
    let rows: Vec<(String,)> = transaction.fetch_all(statement).await?;
    // Chosen here, so it is the same whatever the database's locale.
    let paths = rows
        .iter()
        .map(|(path,)| decode_path(path))
        .collect::<Result<Vec<_>, Error>>()?;
    Ok(paths.into_iter().min())
}

/// Tombstones the live record `id` at its revision `current`: its path is
/// free from here on, and its revisions stay.
pub(crate) async fn tombstone(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    id: &SharedToolId,
    current: u64,
    removed_at_unix_nanos: i64,
) -> Result<(), Error> {
    let statement = Query::update()
        .table(SharedToolRecords::Table)
        .value(SharedToolRecords::RemovedAtUnixNanos, removed_at_unix_nanos)
        .and_where(Expr::col(SharedToolRecords::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SharedToolRecords::Id).eq(id.to_string()))
        .and_where(Expr::col(SharedToolRecords::CurrentRevision).eq(stored_revision(current)?))
        .and_where(Expr::col(SharedToolRecords::RemovedAtUnixNanos).is_null())
        .to_owned();
    if transaction.execute_change(statement).await? != 1 {
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
    let (source_private_tool_id, restored_from_revision) = match revision.origin() {
        SharedToolRevisionOrigin::PrivateTool(private_tool) => {
            (Some(private_tool.to_string()), None)
        }
        SharedToolRevisionOrigin::Revision(restored) => (None, Some(stored_revision(*restored)?)),
    };
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
            Expr::val(AuthoredToolDeclarationFieldProjection::format()),
            Expr::val(projection.status()),
            Expr::val(projection.fields().map(serde_json::to_string).transpose()?),
            Expr::val(projection.failure_reason()),
            Expr::val(serde_json::to_string(revision.diagnostics())?),
            Expr::val(revision.diagnostics_truncated()),
            Expr::val(revision.content_hash()),
            Expr::val(revision.default_access_state().as_storage_str()),
            Expr::val(revision.published_by().as_str()),
            Expr::val(source_private_tool_id),
            Expr::val(restored_from_revision),
            Expr::val(revision.published_at_unix_nanos()),
        ])
        .to_owned();
    transaction.execute_change(statement).await?;
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
        transaction.execute_change(statement).await?;
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
            Expr::val(SharedToolLifecycleEventId::generate().to_string()),
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
    transaction.execute_change(statement).await?;
    Ok(())
}

#[derive(Debug, FromRow)]
struct EventRow {
    id: String,
    actor_user_id: String,
    action: String,
    target_shared_tool_id: String,
    target_path: String,
    revision: i64,
    content_hash: String,
    occurred_at_unix_nanos: i64,
}

impl EventRow {
    fn decode(self) -> Result<SharedToolLifecycleEvent, Error> {
        Ok(SharedToolLifecycleEvent {
            id: SharedToolLifecycleEventId::parse(&self.id).map_err(|_error| {
                Error::CorruptData("Shared Tool lifecycle Event has invalid identity".to_string())
            })?,
            action: SharedToolLifecycleAction::from_storage(&self.action)?,
            actor: decode_user(&self.actor_user_id)?,
            shared_tool_id: decode_id(&self.target_shared_tool_id)?,
            path: decode_path(&self.target_path)?,
            revision: decode_revision(self.revision)?,
            content_hash: self.content_hash,
            occurred_at_unix_nanos: self.occurred_at_unix_nanos,
        })
    }
}

/// One page of the Workspace's lifecycle Events, newest first: those of the
/// Shared Tool `shared_tool` alone, or of every Shared Tool, live and
/// removed. Events that occurred together follow one another in identity
/// order.
pub(crate) async fn list_lifecycle_event_page(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    shared_tool: Option<&SharedToolId>,
    request: &PageRequest<SharedToolLifecycleEventKey>,
) -> Result<Page<SharedToolLifecycleEvent, SharedToolLifecycleEventKey>, Error> {
    let mut statement = Query::select()
        .columns([
            SharedToolLifecycleEvents::Id,
            SharedToolLifecycleEvents::ActorUserId,
            SharedToolLifecycleEvents::Action,
            SharedToolLifecycleEvents::TargetSharedToolId,
            SharedToolLifecycleEvents::TargetPath,
            SharedToolLifecycleEvents::Revision,
            SharedToolLifecycleEvents::ContentHash,
            SharedToolLifecycleEvents::OccurredAtUnixNanos,
        ])
        .from(SharedToolLifecycleEvents::Table)
        .and_where(Expr::col(SharedToolLifecycleEvents::WorkspaceId).eq(workspace_id.to_string()))
        .order_by(SharedToolLifecycleEvents::OccurredAtUnixNanos, Order::Desc)
        .order_by(SharedToolLifecycleEvents::Id, Order::Desc)
        .limit(request.size.lookahead_limit())
        .to_owned();
    if let Some(shared_tool) = shared_tool {
        statement.and_where(
            Expr::col(SharedToolLifecycleEvents::TargetSharedToolId).eq(shared_tool.to_string()),
        );
    }
    if let Some(after) = &request.after {
        statement.and_where(
            Expr::col(SharedToolLifecycleEvents::OccurredAtUnixNanos)
                .lt(after.occurred_at_unix_nanos)
                .or(Expr::col(SharedToolLifecycleEvents::OccurredAtUnixNanos)
                    .eq(after.occurred_at_unix_nanos)
                    .and(Expr::col(SharedToolLifecycleEvents::Id).lt(after.id.to_string()))),
        );
    }
    let rows: Vec<EventRow> = transaction.fetch_all(statement).await?;
    let events = rows
        .into_iter()
        .map(EventRow::decode)
        .collect::<Result<Vec<_>, Error>>()?;
    Ok(Page::from_lookahead(
        events,
        request.size,
        SharedToolLifecycleEvent::key,
    ))
}

/// A Shared Tool's first revision: the publish that made it.
const FIRST_REVISION: i64 = 1;

/// Counts the live Shared Tools, across every Workspace. A removed one is a
/// tombstone and does not count, and neither does a revision. The caller
/// authorizes the read.
///
/// # Errors
///
/// Returns a database failure.
pub async fn shared_tool_total(tx: &mut Transaction<'_>) -> Result<u64, Error> {
    let total = Query::select()
        .expr(Expr::col(SharedToolRecords::Id).count())
        .from(SharedToolRecords::Table)
        .and_where(Expr::col(SharedToolRecords::RemovedAtUnixNanos).is_null())
        .to_owned();
    count_of(tx, total).await
}

/// Counts the Shared Tools first published in `[start, end)` unix
/// nanoseconds, across every Workspace: the publishes that made their first
/// revision. A new revision or a restore does not count. The caller
/// authorizes the read.
///
/// # Errors
///
/// Returns a database failure.
pub async fn shared_tools_published(
    tx: &mut Transaction<'_>,
    start: i64,
    end: i64,
) -> Result<u64, Error> {
    let mut events = lifecycle_events_in(SharedToolLifecycleAction::Published, start, end);
    events.and_where(Expr::col(SharedToolLifecycleEvents::Revision).eq(FIRST_REVISION));
    count_of(tx, events).await
}

/// Counts the Shared Tools removed in `[start, end)` unix nanoseconds,
/// across every Workspace. The caller authorizes the read.
///
/// # Errors
///
/// Returns a database failure.
pub async fn shared_tools_removed(
    tx: &mut Transaction<'_>,
    start: i64,
    end: i64,
) -> Result<u64, Error> {
    let events = lifecycle_events_in(SharedToolLifecycleAction::Removed, start, end);
    count_of(tx, events).await
}

/// A query that counts the lifecycle Events of `action` in `[start, end)`.
fn lifecycle_events_in(action: SharedToolLifecycleAction, start: i64, end: i64) -> SelectStatement {
    Query::select()
        .expr(Expr::col(SharedToolLifecycleEvents::Id).count())
        .from(SharedToolLifecycleEvents::Table)
        .and_where(Expr::col(SharedToolLifecycleEvents::Action).eq(action.as_storage_str()))
        .and_where(Expr::col(SharedToolLifecycleEvents::OccurredAtUnixNanos).gte(start))
        .and_where(Expr::col(SharedToolLifecycleEvents::OccurredAtUnixNanos).lt(end))
        .to_owned()
}

/// The one count a counting query returns.
async fn count_of(tx: &mut Transaction<'_>, query: SelectStatement) -> Result<u64, Error> {
    let counted: Option<(i64,)> = tx.fetch_optional(query).await?;
    Ok(counted.map_or(0, |(counted,)| u64::try_from(counted).unwrap_or_default()))
}
