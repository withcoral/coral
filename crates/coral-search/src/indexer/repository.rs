//! The indexers' statements over the search-owned tables.
//!
//! Every function joins the caller's transaction; [`crate::SearchIndex`] owns
//! when each transaction begins, commits, or rolls back.

use std::collections::{HashMap, HashSet};

use coral_catalog::AuthoredToolKind;
use coral_catalog::{ProviderGenerationId, ToolKey};
use coral_db::{DatabaseSystem, Error, Transaction};
use coral_provider::ProviderId;
use coral_tools::AuthoredToolId;
use coral_workspace::{Principal, WorkspaceId};
use sea_query::{
    Alias, Asterisk, BinOper, Cond, DeleteStatement, Expr, ExprTrait as _, Func, Iden, JoinType,
    OnConflict, Query, SimpleExpr,
};

use super::retry::RetryRule;
use super::{INDEXER_REVISION, IndexError};
use crate::{CallPath, SearchIndexFailure, SearchIndexState};

/// The `search_entities` table and its columns, for sea-query. Each row is
/// one Search Entity that indexed: a Provider Tool, a Private Tool or a Shared Tool.
#[derive(Clone, Copy, Iden)]
enum SearchEntities {
    Table,
    WorkspaceId,
    DocId,
    Kind,
    ProviderId,
    ProviderGenerationId,
    ToolKey,
    AuthoredToolId,
    PrincipalId,
    PrincipalKind,
    CallPath,
    QualifiedName,
    Title,
    Description,
    ProviderName,
    SearchableText,
    Deprecated,
    Fingerprint,
    /// When the row last changed: the attempt that wrote its values.
    IndexedAtUnixNanos,
}

/// The `search_index_states` table and its columns, for sea-query. Each row
/// is the state of one indexing attempt: one per Generation attempt, one per
/// failed Authored Tool attempt.
#[derive(Clone, Copy, Iden)]
enum SearchIndexStates {
    Table,
    WorkspaceId,
    TargetId,
    Kind,
    ProviderId,
    ProviderGenerationId,
    ProviderDisplayName,
    AuthoredToolId,
    PrincipalId,
    PrincipalKind,
    State,
    AttemptedAtUnixNanos,
    Error,
    /// The kind of error a failed Generation attempt recorded.
    ErrorKind,
    /// The indexer revision that wrote a Generation's state and rows.
    IndexerRevision,
    EverVisible,
    UnavailableSinceUnixNanos,
    /// The columns of a failed attempt's [`RetryRule`], [`RETRY_COLUMNS`].
    FailedAttempts,
    RetryAfterUnixNanos,
    FailedCoralVersion,
    /// The revision a failed Authored Tool attempt tried.
    FailedAuthoredToolRevision,
}

/// The columns of a failed attempt's [`RetryRule`], in [`retry_values`]
/// order.
const RETRY_COLUMNS: [SearchIndexStates; 3] = [
    SearchIndexStates::FailedAttempts,
    SearchIndexStates::RetryAfterUnixNanos,
    SearchIndexStates::FailedCoralVersion,
];

/// `columns`, then the [`RETRY_COLUMNS`].
fn with_retry_columns<const N: usize>(
    columns: [SearchIndexStates; N],
) -> impl Iterator<Item = SearchIndexStates> {
    columns.into_iter().chain(RETRY_COLUMNS)
}

/// The [`RETRY_COLUMNS`] as a read returns them, flattened into a row.
#[derive(sqlx::FromRow)]
struct RetryColumns {
    failed_attempts: Option<i64>,
    retry_after_unix_nanos: Option<i64>,
    failed_coral_version: Option<String>,
}

impl From<RetryColumns> for RetryRule {
    fn from(columns: RetryColumns) -> Self {
        Self {
            attempts: columns.failed_attempts,
            retry_after_unix_nanos: columns.retry_after_unix_nanos,
            coral_version: columns.failed_coral_version,
        }
    }
}

/// The Postgres `search_stats` table and the columns the reconcile pass
/// reads, for sea-query. Each row holds one Workspace's ranking totals and
/// the watermark of the count that wrote them.
#[derive(Iden)]
enum SearchStats {
    Table,
    WorkspaceId,
    CountedRows,
    CountedMaxIndexedAt,
}

const TOOL: &str = "provider_tool";

/// The `kind` value of the rows of an Authored Tool of `kind`.
const fn authored_tool(kind: AuthoredToolKind) -> &'static str {
    crate::EntityKind::authored(kind).as_str()
}

/// The `kind` value of every Authored Tool's rows.
const AUTHORED_TOOL_KINDS: [&str; 2] = [
    authored_tool(AuthoredToolKind::Private),
    authored_tool(AuthoredToolKind::Shared),
];
const COMPLETE: &str = "complete";
const FAILED: &str = "failed";

/// Rows per upsert or delete statement, well under each backend's bind
/// limit.
const CHUNK_ROWS: usize = 500;

/// The class of the Postgres advisory lock on one Workspace's search index
/// writes, ASCII `srch`. The second key is the hash of the Workspace id. The
/// two-key form never conflicts with the one-key lock of the stream steps.
const WORKSPACE_LOCK_CLASS: i32 = 0x7372_6368;

/// The class of the Postgres advisory lock on one Workspace's statistics
/// recounts, ASCII `srst`, keyed as [`WORKSPACE_LOCK_CLASS`] is.
const STATISTICS_LOCK_CLASS: i32 = 0x7372_7374;

/// On Postgres, takes the transaction-scoped lock on the Workspace's search
/// index writes, so two processes that write one Workspace take turns.
/// `Database::begin` runs at the server's default isolation, read committed,
/// so each later statement of the transaction sees what the previous holder
/// committed. The writes of one Workspace therefore commit in the order of
/// their stamps, and each write stamps its rows later than every row that a
/// statistics count could have seen before it: the statistics watermark
/// relies on that. `SQLite` serializes every write already.
pub(super) async fn lock_workspace(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<(), Error> {
    advisory_lock(transaction, WORKSPACE_LOCK_CLASS, workspace_id).await
}

/// On Postgres, takes the transaction-scoped lock on the Workspace's
/// statistics recounts, so two recounts of one Workspace take turns and
/// neither meets the other's term rows. A recount takes no write lock: its
/// one statement counts from one snapshot, and the watermark it stores shows
/// each write that commits after that snapshot. So a write never waits for a
/// recount. `SQLite` keeps no statistics.
pub(super) async fn lock_statistics(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<(), Error> {
    advisory_lock(transaction, STATISTICS_LOCK_CLASS, workspace_id).await
}

async fn advisory_lock(
    transaction: &mut Transaction<'_>,
    class: i32,
    workspace_id: &WorkspaceId,
) -> Result<(), Error> {
    if transaction.system() != DatabaseSystem::Postgres {
        return Ok(());
    }
    transaction
        .execute_sql(
            "SELECT pg_advisory_xact_lock($1, hashtext($2))",
            sea_query::Values(vec![class.into(), workspace_id.to_string().into()]),
        )
        .await?;
    Ok(())
}

/// The columns a Tool row sets, in [`ToolRow::values`] order. The
/// first two are the key.
const TOOL_COLUMNS: [SearchEntities; 15] = [
    SearchEntities::WorkspaceId,
    SearchEntities::DocId,
    SearchEntities::Kind,
    SearchEntities::ProviderId,
    SearchEntities::ProviderGenerationId,
    SearchEntities::ToolKey,
    SearchEntities::CallPath,
    SearchEntities::QualifiedName,
    SearchEntities::Title,
    SearchEntities::Description,
    SearchEntities::ProviderName,
    SearchEntities::SearchableText,
    SearchEntities::Deprecated,
    SearchEntities::Fingerprint,
    SearchEntities::IndexedAtUnixNanos,
];

/// One Tool's row in `search_entities`. The document id and the
/// fingerprint derive from the Generation and the Tool Key. The write
/// stamps the row when it upserts it.
pub(crate) struct ToolRow<'a> {
    pub(super) workspace_id: &'a WorkspaceId,
    pub(super) provider_id: &'a ProviderId,
    pub(super) generation_id: &'a ProviderGenerationId,
    pub(super) tool_key: &'a ToolKey,
    pub(super) call_path: CallPath,
    pub(super) qualified_name: &'a str,
    pub(super) title: String,
    pub(super) description: String,
    pub(super) provider_name: String,
    pub(super) searchable_text: String,
    pub(super) deprecated: bool,
}

impl ToolRow<'_> {
    fn doc_id(&self) -> String {
        format!("{}:{}", self.generation_id, self.tool_key.as_str())
    }

    fn values(&self, indexed_at_unix_nanos: i64) -> [SimpleExpr; TOOL_COLUMNS.len()] {
        let generation_id = self.generation_id.to_string();
        [
            self.workspace_id.to_string().into(),
            self.doc_id().into(),
            TOOL.into(),
            self.provider_id.to_string().into(),
            generation_id.clone().into(),
            self.tool_key.as_str().into(),
            self.call_path.as_str().into(),
            self.qualified_name.into(),
            self.title.clone().into(),
            self.description.clone().into(),
            self.provider_name.clone().into(),
            self.searchable_text.clone().into(),
            self.deprecated.into(),
            generation_id.into(),
            indexed_at_unix_nanos.into(),
        ]
    }
}

/// The outcome an index attempt records.
pub(super) enum AttemptState {
    Complete,
    Failed {
        error: String,
        failure: Option<SearchIndexFailure>,
        retry: RetryRule,
    },
}

/// One Generation's row in `search_index_states`.
pub(super) struct GenerationState<'a> {
    pub(super) workspace_id: &'a WorkspaceId,
    pub(super) provider_id: &'a ProviderId,
    pub(super) generation_id: &'a ProviderGenerationId,
    /// The Provider display name the attempt indexed.
    pub(super) display_name: &'a str,
    pub(super) attempted_at_unix_nanos: i64,
    pub(super) state: AttemptState,
}

/// Upserts Tool rows by Workspace and document id, stamped with
/// `indexed_at_unix_nanos`, as [`changed_rows_only`] describes. An update,
/// never `INSERT OR REPLACE`: on `SQLite` a replace skips the delete trigger
/// and leaves a stale copy in the FTS5 table.
pub(super) async fn upsert_tool_rows(
    transaction: &mut Transaction<'_>,
    rows: &[ToolRow<'_>],
    indexed_at_unix_nanos: i64,
) -> Result<(), Error> {
    for chunk in rows.chunks(CHUNK_ROWS) {
        let mut statement = Query::insert();
        statement
            .into_table(SearchEntities::Table)
            .columns(TOOL_COLUMNS);
        for row in chunk {
            statement.values_panic(row.values(indexed_at_unix_nanos));
        }
        statement.on_conflict(changed_rows_only(TOOL_COLUMNS));
        transaction.execute(statement).await?;
    }
    Ok(())
}

/// The conflict action of a row upsert: it updates a conflicting row only
/// when the insert brings another value for a column other than the key and
/// the stamp. A row whose values stay the same keeps its stamp and its
/// version, and no trigger fires for it. On Postgres the update recomputes
/// the row's generated vector and length, so each row takes one write.
///
/// Both databases compare with `IS DISTINCT FROM`, so two NULLs are equal.
/// `SQLite` has it from 3.39; the bundled build is newer.
///
/// `columns` opens with the two key columns and ends with the stamp.
fn changed_rows_only<const N: usize>(columns: [SearchEntities; N]) -> OnConflict {
    let changed = columns
        .into_iter()
        .skip(2)
        .filter(|column| !matches!(column, SearchEntities::IndexedAtUnixNanos))
        .fold(Cond::any(), |changed, column| {
            changed.add(Expr::col((SearchEntities::Table, column)).binary(
                BinOper::Custom("IS DISTINCT FROM"),
                Expr::col((Alias::new("excluded"), column)),
            ))
        });
    let mut on_conflict = OnConflict::columns([SearchEntities::WorkspaceId, SearchEntities::DocId]);
    on_conflict.update_columns(columns.into_iter().skip(2));
    on_conflict.action_and_where(changed.into());
    on_conflict
}

/// Deletes the Generation's rows whose document id is not among `rows`, the
/// rows an attempt wrote or kept.
pub(super) async fn delete_rows_not_written(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    generation_id: &ProviderGenerationId,
    rows: &[ToolRow<'_>],
) -> Result<(), Error> {
    let written = rows.iter().map(ToolRow::doc_id).collect::<HashSet<_>>();
    let indexed: Vec<(String,)> = transaction
        .fetch_all(
            Query::select()
                .column(SearchEntities::DocId)
                .from(SearchEntities::Table)
                .and_where(Expr::col(SearchEntities::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(
                    Expr::col(SearchEntities::ProviderGenerationId).eq(generation_id.to_string()),
                )
                .to_owned(),
        )
        .await?;
    let stale = indexed
        .into_iter()
        .map(|(doc_id,)| doc_id)
        .filter(|doc_id| !written.contains(doc_id))
        .collect::<Vec<_>>();
    for chunk in stale.chunks(CHUNK_ROWS) {
        transaction
            .execute(
                generation_rows(workspace_id, generation_id)
                    .and_where(Expr::col(SearchEntities::DocId).is_in(chunk.iter().cloned()))
                    .to_owned(),
            )
            .await?;
    }
    Ok(())
}

/// Deletes every Tool row of the Generation. Returns the rows it deleted.
pub(super) async fn delete_generation_rows(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    generation_id: &ProviderGenerationId,
) -> Result<u64, Error> {
    transaction
        .execute(generation_rows(workspace_id, generation_id))
        .await
}

fn generation_rows(
    workspace_id: &WorkspaceId,
    generation_id: &ProviderGenerationId,
) -> DeleteStatement {
    Query::delete()
        .from_table(SearchEntities::Table)
        .and_where(Expr::col(SearchEntities::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SearchEntities::ProviderGenerationId).eq(generation_id.to_string()))
        .to_owned()
}

/// Upserts the Generation's state row, under this indexer's revision.
pub(super) async fn write_generation_state(
    transaction: &mut Transaction<'_>,
    state: GenerationState<'_>,
) -> Result<(), Error> {
    let (name, error, error_kind, retry) = match state.state {
        AttemptState::Complete => (COMPLETE, None, None, None),
        AttemptState::Failed {
            error,
            failure,
            retry,
        } => (FAILED, Some(error), failure.map(failure_kind), Some(retry)),
    };
    let complete = name == COMPLETE;
    let ever_visible: SimpleExpr = if complete {
        Expr::val(true)
    } else {
        Expr::case(
            Expr::col((SearchIndexStates::Table, SearchIndexStates::State)).eq(COMPLETE),
            true,
        )
        .finally(Expr::col((
            SearchIndexStates::Table,
            SearchIndexStates::EverVisible,
        )))
        .into()
    };
    let unavailable_since: SimpleExpr = if complete {
        Expr::val(Option::<i64>::None)
    } else {
        Expr::case(
            Expr::col((SearchIndexStates::Table, SearchIndexStates::State)).eq(COMPLETE),
            state.attempted_at_unix_nanos,
        )
        .finally(Func::coalesce([
            Expr::col((
                SearchIndexStates::Table,
                SearchIndexStates::UnavailableSinceUnixNanos,
            )),
            Expr::val(state.attempted_at_unix_nanos),
        ]))
        .into()
    };
    transaction
        .execute(
            Query::insert()
                .into_table(SearchIndexStates::Table)
                .columns(with_retry_columns([
                    SearchIndexStates::WorkspaceId,
                    SearchIndexStates::TargetId,
                    SearchIndexStates::Kind,
                    SearchIndexStates::ProviderId,
                    SearchIndexStates::ProviderGenerationId,
                    SearchIndexStates::ProviderDisplayName,
                    SearchIndexStates::State,
                    SearchIndexStates::AttemptedAtUnixNanos,
                    SearchIndexStates::Error,
                    SearchIndexStates::ErrorKind,
                    SearchIndexStates::IndexerRevision,
                    SearchIndexStates::EverVisible,
                    SearchIndexStates::UnavailableSinceUnixNanos,
                ]))
                .values_panic(
                    [
                        state.workspace_id.to_string().into(),
                        state.generation_id.to_string().into(),
                        TOOL.into(),
                        state.provider_id.to_string().into(),
                        state.generation_id.to_string().into(),
                        state.display_name.into(),
                        name.into(),
                        state.attempted_at_unix_nanos.into(),
                        error.into(),
                        error_kind.into(),
                        INDEXER_REVISION.into(),
                        complete.into(),
                        (!complete).then_some(state.attempted_at_unix_nanos).into(),
                    ]
                    .into_iter()
                    .chain(retry_values(retry.as_ref())),
                )
                .on_conflict(
                    OnConflict::column(SearchIndexStates::TargetId)
                        .update_columns(with_retry_columns([
                            SearchIndexStates::ProviderDisplayName,
                            SearchIndexStates::State,
                            SearchIndexStates::AttemptedAtUnixNanos,
                            SearchIndexStates::Error,
                            SearchIndexStates::ErrorKind,
                            SearchIndexStates::IndexerRevision,
                        ]))
                        .value(SearchIndexStates::EverVisible, ever_visible)
                        .value(
                            SearchIndexStates::UnavailableSinceUnixNanos,
                            unavailable_since,
                        )
                        .to_owned(),
                )
                .to_owned(),
        )
        .await?;
    Ok(())
}

/// Deletes one Generation's Tool rows and state row. Returns the Workspace
/// its state row named when Tool rows went, `None` when none went or the
/// index held no state for the Generation: the next reconcile pass then sees
/// the changed rows.
pub(super) async fn delete_generation(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
) -> Result<Option<WorkspaceId>, IndexError> {
    let generation_id = generation_id.to_string();
    let workspace: Option<(String,)> = transaction
        .fetch_optional(
            Query::select()
                .column(SearchIndexStates::WorkspaceId)
                .from(SearchIndexStates::Table)
                .and_where(Expr::col(SearchIndexStates::Kind).eq(TOOL))
                .and_where(Expr::col(SearchIndexStates::TargetId).eq(generation_id.as_str()))
                .to_owned(),
        )
        .await?;
    let removed = transaction
        .execute(
            Query::delete()
                .from_table(SearchEntities::Table)
                .and_where(Expr::col(SearchEntities::Kind).eq(TOOL))
                .and_where(
                    Expr::col(SearchEntities::ProviderGenerationId).eq(generation_id.as_str()),
                )
                .to_owned(),
        )
        .await?;
    transaction
        .execute(
            Query::delete()
                .from_table(SearchIndexStates::Table)
                .and_where(Expr::col(SearchIndexStates::Kind).eq(TOOL))
                .and_where(
                    Expr::col(SearchIndexStates::ProviderGenerationId).eq(generation_id.as_str()),
                )
                .to_owned(),
        )
        .await?;
    workspace
        .filter(|_workspace| removed > 0)
        .map(|(workspace_id,)| parse_workspace_id(&workspace_id))
        .transpose()
}

/// Deletes the Tool rows and state rows of every Generation of one
/// Provider. Returns the Tool rows it deleted.
pub(super) async fn delete_provider(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider_id: &ProviderId,
) -> Result<u64, Error> {
    let workspace_id = workspace_id.to_string();
    let provider_id = provider_id.to_string();
    let removed = transaction
        .execute(
            Query::delete()
                .from_table(SearchEntities::Table)
                .and_where(Expr::col(SearchEntities::Kind).eq(TOOL))
                .and_where(Expr::col(SearchEntities::WorkspaceId).eq(workspace_id.as_str()))
                .and_where(Expr::col(SearchEntities::ProviderId).eq(provider_id.as_str()))
                .to_owned(),
        )
        .await?;
    transaction
        .execute(
            Query::delete()
                .from_table(SearchIndexStates::Table)
                .and_where(Expr::col(SearchIndexStates::Kind).eq(TOOL))
                .and_where(Expr::col(SearchIndexStates::WorkspaceId).eq(workspace_id.as_str()))
                .and_where(Expr::col(SearchIndexStates::ProviderId).eq(provider_id.as_str()))
                .to_owned(),
        )
        .await?;
    Ok(removed)
}

/// The columns an Authored Tool row sets, in [`AuthoredToolRow::values`] order.
/// The first two are the key.
const AUTHORED_TOOL_COLUMNS: [SearchEntities; 15] = [
    SearchEntities::WorkspaceId,
    SearchEntities::DocId,
    SearchEntities::Kind,
    SearchEntities::AuthoredToolId,
    SearchEntities::PrincipalId,
    SearchEntities::PrincipalKind,
    SearchEntities::CallPath,
    SearchEntities::QualifiedName,
    SearchEntities::Title,
    SearchEntities::Description,
    SearchEntities::ProviderName,
    SearchEntities::SearchableText,
    SearchEntities::Deprecated,
    SearchEntities::Fingerprint,
    SearchEntities::IndexedAtUnixNanos,
];

/// One Authored Tool's row in `search_entities`. The document id is the
/// Tool's id; the fingerprint is [`authored_tool_fingerprint`] of its
/// revision. Only a Private Tool's row names a Principal, its owner. The
/// write stamps the row when it upserts it.
pub(crate) struct AuthoredToolRow<'a> {
    pub(super) workspace_id: &'a WorkspaceId,
    pub(super) id: &'a AuthoredToolId,
    pub(super) principal: Option<&'a Principal>,
    pub(super) call_path: CallPath,
    pub(super) qualified_name: &'a str,
    pub(super) description: &'a str,
    pub(super) searchable_text: String,
    pub(super) revision: u64,
}

impl AuthoredToolRow<'_> {
    fn values(&self, indexed_at_unix_nanos: i64) -> [SimpleExpr; AUTHORED_TOOL_COLUMNS.len()] {
        let id = self.id.to_string();
        let [principal_id, principal_kind] = principal_values(self.principal);
        [
            self.workspace_id.to_string().into(),
            id.clone().into(),
            authored_tool(self.id.kind()).into(),
            id.into(),
            principal_id,
            principal_kind,
            self.call_path.as_str().into(),
            self.qualified_name.into(),
            "".into(),
            self.description.into(),
            "".into(),
            self.searchable_text.clone().into(),
            false.into(),
            authored_tool_fingerprint(self.revision).into(),
            indexed_at_unix_nanos.into(),
        ]
    }
}

/// The `principal_id` and `principal_kind` values of an Authored Tool's row:
/// those of a Private Tool's owner, and `NULL` for a Shared Tool.
fn principal_values(principal: Option<&Principal>) -> [SimpleExpr; 2] {
    [
        principal.map(|principal| principal.id().as_str()).into(),
        principal
            .map(|principal| principal.kind().as_storage_str())
            .into(),
    ]
}

/// The fingerprint of an Authored Tool row: the Tool's revision and the
/// indexer revision, as `3.1`. A row that an older indexer revision wrote
/// holds another fingerprint, so a reconcile pass indexes it again. A Coral
/// without indexer revisions wrote the definition revision alone.
pub(super) fn authored_tool_fingerprint(revision: u64) -> String {
    format!("{revision}.{INDEXER_REVISION}")
}

/// One failed Authored Tool attempt's row in `search_index_states`.
pub(super) struct FailedAuthoredToolState<'a> {
    pub(super) workspace_id: &'a WorkspaceId,
    pub(super) id: &'a AuthoredToolId,
    pub(super) principal: Option<&'a Principal>,
    pub(super) attempted_at_unix_nanos: i64,
    pub(super) error: String,
    pub(super) retry: RetryRule,
    /// The revision the attempt tried.
    pub(super) revision: u64,
}

/// Upserts an Authored Tool row by Workspace and document id, stamped with
/// `indexed_at_unix_nanos`, as [`changed_rows_only`] describes. An update,
/// never `INSERT OR REPLACE`, for the reason [`upsert_tool_rows`]
/// gives.
pub(super) async fn upsert_authored_tool_row(
    transaction: &mut Transaction<'_>,
    row: &AuthoredToolRow<'_>,
    indexed_at_unix_nanos: i64,
) -> Result<(), Error> {
    let on_conflict = changed_rows_only(AUTHORED_TOOL_COLUMNS);
    transaction
        .execute(
            Query::insert()
                .into_table(SearchEntities::Table)
                .columns(AUTHORED_TOOL_COLUMNS)
                .values_panic(row.values(indexed_at_unix_nanos))
                .on_conflict(on_conflict)
                .to_owned(),
        )
        .await?;
    Ok(())
}

/// Deletes an Authored Tool's row and state row. Returns the rows it
/// deleted from the search table.
pub(super) async fn delete_authored_tool_entry(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    id: &AuthoredToolId,
) -> Result<u64, Error> {
    let removed = transaction
        .execute(authored_tool_row(workspace_id, id))
        .await?;
    delete_authored_tool_state(transaction, workspace_id, id).await?;
    Ok(removed)
}

fn authored_tool_row(workspace_id: &WorkspaceId, id: &AuthoredToolId) -> DeleteStatement {
    Query::delete()
        .from_table(SearchEntities::Table)
        .and_where(Expr::col(SearchEntities::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SearchEntities::DocId).eq(id.to_string()))
        .and_where(Expr::col(SearchEntities::Kind).eq(authored_tool(id.kind())))
        .to_owned()
}

/// Deletes an Authored Tool's row. Returns the rows it deleted.
pub(super) async fn delete_authored_tool_row(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    id: &AuthoredToolId,
) -> Result<u64, Error> {
    transaction
        .execute(authored_tool_row(workspace_id, id))
        .await
}

/// Upserts an Authored Tool's `failed` state row.
pub(super) async fn write_failed_authored_tool_state(
    transaction: &mut Transaction<'_>,
    state: FailedAuthoredToolState<'_>,
) -> Result<(), Error> {
    let id = state.id.to_string();
    let [principal_id, principal_kind] = principal_values(state.principal);
    transaction
        .execute(
            Query::insert()
                .into_table(SearchIndexStates::Table)
                .columns(with_retry_columns([
                    SearchIndexStates::WorkspaceId,
                    SearchIndexStates::TargetId,
                    SearchIndexStates::Kind,
                    SearchIndexStates::AuthoredToolId,
                    SearchIndexStates::PrincipalId,
                    SearchIndexStates::PrincipalKind,
                    SearchIndexStates::State,
                    SearchIndexStates::AttemptedAtUnixNanos,
                    SearchIndexStates::Error,
                    SearchIndexStates::FailedAuthoredToolRevision,
                ]))
                .values_panic(
                    [
                        state.workspace_id.to_string().into(),
                        id.clone().into(),
                        authored_tool(state.id.kind()).into(),
                        id.into(),
                        principal_id,
                        principal_kind,
                        FAILED.into(),
                        state.attempted_at_unix_nanos.into(),
                        state.error.into(),
                        revision_value(state.revision),
                    ]
                    .into_iter()
                    .chain(retry_values(Some(&state.retry))),
                )
                .on_conflict(
                    OnConflict::column(SearchIndexStates::TargetId)
                        .update_columns(with_retry_columns([
                            SearchIndexStates::AttemptedAtUnixNanos,
                            SearchIndexStates::Error,
                            SearchIndexStates::FailedAuthoredToolRevision,
                        ]))
                        .to_owned(),
                )
                .to_owned(),
        )
        .await?;
    Ok(())
}

/// The [`RETRY_COLUMNS`] values of a failed attempt's retry rule, all
/// `NULL` without one.
fn retry_values(retry: Option<&RetryRule>) -> [SimpleExpr; 3] {
    match retry {
        Some(retry) => [
            retry.attempts.into(),
            retry.retry_after_unix_nanos.into(),
            retry.coral_version.clone().into(),
        ],
        None => [
            Option::<i64>::None.into(),
            Option::<i64>::None.into(),
            Option::<String>::None.into(),
        ],
    }
}

/// An Authored Tool's revision as its `BIGINT` column holds it. A revision past
/// the column's range is stored as `NULL`, which matches no revision.
fn revision_value(revision: u64) -> SimpleExpr {
    i64::try_from(revision).ok().into()
}

/// What a target's `failed` state row records for its next attempt.
#[derive(sqlx::FromRow)]
struct FailedStateRow {
    failed_authored_tool_revision: Option<i64>,
    #[sqlx(flatten)]
    retry: RetryColumns,
}

/// Reads the target's state row in the Workspace, when its last attempt
/// failed. `kind` is the state row's kind.
async fn failed_state(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    kind: &str,
    target_id: String,
) -> Result<Option<FailedStateRow>, Error> {
    transaction
        .fetch_optional(
            Query::select()
                .columns(with_retry_columns([
                    SearchIndexStates::FailedAuthoredToolRevision,
                ]))
                .from(SearchIndexStates::Table)
                .and_where(Expr::col(SearchIndexStates::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(SearchIndexStates::Kind).eq(kind))
                .and_where(Expr::col(SearchIndexStates::TargetId).eq(target_id))
                .and_where(Expr::col(SearchIndexStates::State).eq(FAILED))
                .to_owned(),
        )
        .await
}

/// Reads the retry rule of the Generation's state row, when its last
/// attempt failed.
pub(super) async fn failed_generation_retry_rule(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    generation_id: &ProviderGenerationId,
) -> Result<Option<RetryRule>, Error> {
    let row = failed_state(transaction, workspace_id, TOOL, generation_id.to_string()).await?;
    Ok(row.map(|row| row.retry.into()))
}

/// The failure an Authored Tool's state row records.
pub(super) struct AuthoredToolFailure {
    pub(super) retry: RetryRule,
    /// The revision the attempt tried. `None` on a row written
    /// before Coral recorded it.
    pub(super) revision: Option<u64>,
}

impl From<FailedStateRow> for AuthoredToolFailure {
    fn from(row: FailedStateRow) -> Self {
        Self {
            retry: row.retry.into(),
            revision: row
                .failed_authored_tool_revision
                .and_then(|revision| u64::try_from(revision).ok()),
        }
    }
}

/// Reads the failure of the Authored Tool's state row, when its last
/// attempt failed.
pub(super) async fn failed_authored_tool(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    id: &AuthoredToolId,
) -> Result<Option<AuthoredToolFailure>, Error> {
    let row = failed_state(
        transaction,
        workspace_id,
        authored_tool(id.kind()),
        id.to_string(),
    )
    .await?;
    Ok(row.map(AuthoredToolFailure::from))
}

/// Deletes an Authored Tool's state row, if an earlier attempt left one.
pub(super) async fn delete_authored_tool_state(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    id: &AuthoredToolId,
) -> Result<(), Error> {
    transaction
        .execute(
            Query::delete()
                .from_table(SearchIndexStates::Table)
                .and_where(Expr::col(SearchIndexStates::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(SearchIndexStates::Kind).eq(authored_tool(id.kind())))
                .and_where(Expr::col(SearchIndexStates::TargetId).eq(id.to_string()))
                .to_owned(),
        )
        .await?;
    Ok(())
}

/// One Generation in its Workspace.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct WorkspaceGeneration {
    pub(super) workspace_id: WorkspaceId,
    pub(super) generation_id: ProviderGenerationId,
}

/// One Generation's state row, as a reconcile pass compares it.
pub(super) struct RecordedGeneration {
    pub(super) generation: WorkspaceGeneration,
    pub(super) outcome: RecordedOutcome,
    /// The indexer revision that wrote the row; `None` when a Coral without
    /// indexer revisions wrote it.
    pub(super) indexer_revision: Option<i64>,
    pub(super) visibility: RecordedVisibility,
}

/// The outcome a Generation's state row records.
pub(super) enum RecordedOutcome {
    /// Indexed under this Provider display name.
    Complete { display_name: String },
    /// Failed, to be retried by this rule.
    Failed { retry: RetryRule },
}

#[derive(sqlx::FromRow)]
struct RecordedGenerationRow {
    workspace_id: String,
    provider_generation_id: String,
    state: String,
    provider_display_name: String,
    indexer_revision: Option<i64>,
    ever_visible: Option<bool>,
    unavailable_since_unix_nanos: Option<i64>,
    #[sqlx(flatten)]
    retry: RetryColumns,
}

/// Durable evidence about whether a catalog has been searchable before.
#[derive(Clone, Copy)]
pub(super) enum RecordedVisibility {
    NeverVisible,
    Visible { unavailable_since: Option<i64> },
    Unknown,
}

fn recorded_visibility(
    state: &str,
    ever_visible: Option<bool>,
    unavailable_since: Option<i64>,
) -> RecordedVisibility {
    if state == COMPLETE {
        return RecordedVisibility::Visible {
            unavailable_since: None,
        };
    }
    match ever_visible {
        Some(true) => RecordedVisibility::Visible { unavailable_since },
        Some(false) => RecordedVisibility::NeverVisible,
        None => RecordedVisibility::Unknown,
    }
}

pub(super) async fn generation_visibility(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    generation: &ProviderGenerationId,
) -> Result<Option<RecordedVisibility>, IndexError> {
    let row: Option<(String, Option<bool>, Option<i64>)> = transaction
        .fetch_optional(
            Query::select()
                .columns([
                    SearchIndexStates::State,
                    SearchIndexStates::EverVisible,
                    SearchIndexStates::UnavailableSinceUnixNanos,
                ])
                .from(SearchIndexStates::Table)
                .and_where(Expr::col(SearchIndexStates::WorkspaceId).eq(workspace.to_string()))
                .and_where(
                    Expr::col(SearchIndexStates::ProviderGenerationId).eq(generation.to_string()),
                )
                .and_where(Expr::col(SearchIndexStates::Kind).eq(TOOL))
                .to_owned(),
        )
        .await?;
    Ok(row.map(|(state, visible, since)| recorded_visibility(&state, visible, since)))
}

/// Selects the Generation state rows [`RecordedGeneration::from_row`] reads.
fn recorded_generation_rows() -> sea_query::SelectStatement {
    Query::select()
        .columns(with_retry_columns([
            SearchIndexStates::WorkspaceId,
            SearchIndexStates::ProviderGenerationId,
            SearchIndexStates::State,
            SearchIndexStates::ProviderDisplayName,
            SearchIndexStates::IndexerRevision,
            SearchIndexStates::EverVisible,
            SearchIndexStates::UnavailableSinceUnixNanos,
        ]))
        .from(SearchIndexStates::Table)
        .and_where(Expr::col(SearchIndexStates::Kind).eq(TOOL))
        .to_owned()
}

impl RecordedGeneration {
    fn from_row(row: RecordedGenerationRow) -> Result<Self, IndexError> {
        // The state check admits only `complete` and `failed`.
        let visibility = recorded_visibility(
            &row.state,
            row.ever_visible,
            row.unavailable_since_unix_nanos,
        );
        let outcome = if row.state == COMPLETE {
            RecordedOutcome::Complete {
                display_name: row.provider_display_name,
            }
        } else {
            RecordedOutcome::Failed {
                retry: row.retry.into(),
            }
        };
        Ok(Self {
            generation: workspace_generation(&row.workspace_id, &row.provider_generation_id)?,
            outcome,
            indexer_revision: row.indexer_revision,
            visibility,
        })
    }
}

/// Reads every Generation's state row in every Workspace.
pub(super) async fn recorded_generations(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<RecordedGeneration>, IndexError> {
    let rows: Vec<RecordedGenerationRow> =
        transaction.fetch_all(recorded_generation_rows()).await?;
    rows.into_iter().map(RecordedGeneration::from_row).collect()
}

/// Reads one Generation's state row, `None` when no attempt has reached it.
pub(super) async fn recorded_generation(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    generation_id: &ProviderGenerationId,
) -> Result<Option<RecordedGeneration>, IndexError> {
    let row: Option<RecordedGenerationRow> = transaction
        .fetch_optional(
            recorded_generation_rows()
                .and_where(Expr::col(SearchIndexStates::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(
                    Expr::col(SearchIndexStates::ProviderGenerationId)
                        .eq(generation_id.to_string()),
                )
                .to_owned(),
        )
        .await?;
    row.map(RecordedGeneration::from_row).transpose()
}

/// Reads the outcome each of a Workspace's Generation state rows records, or
/// only the one Provider's, keyed by Generation. A Generation no attempt has
/// reached has no entry.
pub(crate) async fn generation_outcomes_in_workspace(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider_filter: Option<&ProviderId>,
) -> Result<HashMap<ProviderGenerationId, SearchIndexState>, IndexError> {
    let mut query = Query::select()
        .columns([
            SearchIndexStates::ProviderGenerationId,
            SearchIndexStates::State,
            SearchIndexStates::ErrorKind,
        ])
        .from(SearchIndexStates::Table)
        .and_where(Expr::col(SearchIndexStates::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SearchIndexStates::Kind).eq(TOOL))
        .to_owned();
    if let Some(provider_id) = provider_filter {
        query.and_where(Expr::col(SearchIndexStates::ProviderId).eq(provider_id.to_string()));
    }
    let rows: Vec<(String, String, Option<String>)> = transaction.fetch_all(query).await?;
    rows.into_iter()
        .map(|(generation_id, state, error_kind)| {
            let generation_id = ProviderGenerationId::parse(&generation_id).map_err(|_error| {
                IndexError::Corrupt("a search index state row has an invalid Generation id")
            })?;
            let state = match (state.as_str(), error_kind.as_deref()) {
                (COMPLETE, None) => SearchIndexState::Indexed,
                (FAILED, kind) => SearchIndexState::Failed {
                    reason: kind.and_then(parse_failure_kind),
                },
                _ => {
                    return Err(IndexError::Corrupt(
                        "a search index state row has an invalid state",
                    ));
                }
            };
            Ok((generation_id, state))
        })
        .collect()
}

/// The `error_kind` value of each kind. Exhaustive, so a new kind does not
/// compile without one.
const fn failure_kind(failure: SearchIndexFailure) -> &'static str {
    match failure {
        SearchIndexFailure::InvalidEntry => "invalid_entry",
        SearchIndexFailure::TimedOut => "timed_out",
        SearchIndexFailure::Clock => "clock",
        SearchIndexFailure::Corrupt => "corrupt",
        SearchIndexFailure::Provider => "provider",
        SearchIndexFailure::Database => "database",
    }
}

/// Every kind the read can name. A kind missing here reads as no reason;
/// its test lists every kind with an exhaustive match.
const FAILURES: [SearchIndexFailure; 6] = [
    SearchIndexFailure::InvalidEntry,
    SearchIndexFailure::TimedOut,
    SearchIndexFailure::Clock,
    SearchIndexFailure::Corrupt,
    SearchIndexFailure::Provider,
    SearchIndexFailure::Database,
];

/// `None` for a kind this Coral does not know, such as one a later Coral
/// wrote: the kind is information only, so it never fails the read.
fn parse_failure_kind(value: &str) -> Option<SearchIndexFailure> {
    FAILURES
        .into_iter()
        .find(|failure| failure_kind(*failure) == value)
}

/// Reads a Workspace's Tool rows as `(provider id, Generation id, Call
/// Path, Provider name)`, in Call Path order, for tests in crates that start
/// Coral.
#[cfg(any(test, feature = "test-fixtures"))]
pub(crate) async fn tool_rows_in_workspace(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<Vec<(String, String, String, String)>, Error> {
    transaction
        .fetch_all(
            Query::select()
                .columns([
                    SearchEntities::ProviderId,
                    SearchEntities::ProviderGenerationId,
                    SearchEntities::CallPath,
                    SearchEntities::ProviderName,
                ])
                .from(SearchEntities::Table)
                .and_where(Expr::col(SearchEntities::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(SearchEntities::Kind).eq(TOOL))
                .order_by(SearchEntities::CallPath, sea_query::Order::Asc)
                .to_owned(),
        )
        .await
}

/// Reads a Workspace's Generation state rows as `(provider id, Generation
/// id, state, indexed display name)`, in Generation id order, for tests in
/// crates that start Coral.
#[cfg(any(test, feature = "test-fixtures"))]
pub(crate) async fn generation_states_in_workspace(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<Vec<(String, String, String, Option<String>)>, Error> {
    transaction
        .fetch_all(
            Query::select()
                .columns([
                    SearchIndexStates::ProviderId,
                    SearchIndexStates::ProviderGenerationId,
                    SearchIndexStates::State,
                    SearchIndexStates::ProviderDisplayName,
                ])
                .from(SearchIndexStates::Table)
                .and_where(Expr::col(SearchIndexStates::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(SearchIndexStates::Kind).eq(TOOL))
                .order_by(
                    SearchIndexStates::ProviderGenerationId,
                    sea_query::Order::Asc,
                )
                .to_owned(),
        )
        .await
}

/// Reads every Generation that has Tool rows, each once.
pub(super) async fn generations_with_rows(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<WorkspaceGeneration>, IndexError> {
    let rows: Vec<(String, String)> = transaction
        .fetch_all(
            Query::select()
                .distinct()
                .columns([
                    SearchEntities::WorkspaceId,
                    SearchEntities::ProviderGenerationId,
                ])
                .from(SearchEntities::Table)
                .and_where(Expr::col(SearchEntities::Kind).eq(TOOL))
                .to_owned(),
        )
        .await?;
    rows.iter()
        .map(|(workspace_id, generation_id)| workspace_generation(workspace_id, generation_id))
        .collect()
}

/// Deletes one Generation's Tool rows and state row within its
/// Workspace. Returns the Tool rows it deleted.
pub(super) async fn delete_generation_in_workspace(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    generation_id: &ProviderGenerationId,
) -> Result<u64, Error> {
    let removed = transaction
        .execute(generation_rows(workspace_id, generation_id))
        .await?;
    transaction
        .execute(
            Query::delete()
                .from_table(SearchIndexStates::Table)
                .and_where(Expr::col(SearchIndexStates::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(SearchIndexStates::Kind).eq(TOOL))
                .and_where(
                    Expr::col(SearchIndexStates::ProviderGenerationId)
                        .eq(generation_id.to_string()),
                )
                .to_owned(),
        )
        .await?;
    Ok(removed)
}

/// One Authored Tool in its Workspace.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct WorkspaceAuthoredTool {
    pub(super) workspace_id: WorkspaceId,
    pub(super) id: AuthoredToolId,
}

/// One Authored Tool's row, as a reconcile pass compares it.
pub(super) struct RecordedAuthoredTool {
    pub(super) tool: WorkspaceAuthoredTool,
    /// The row's fingerprint, as [`authored_tool_fingerprint`] writes it.
    pub(super) fingerprint: String,
}

/// Reads the row of every Authored Tool, of either kind, in every
/// Workspace.
pub(super) async fn recorded_authored_tools(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<RecordedAuthoredTool>, IndexError> {
    let rows: Vec<(String, String, String, String)> = transaction
        .fetch_all(
            Query::select()
                .columns([
                    SearchEntities::Kind,
                    SearchEntities::WorkspaceId,
                    SearchEntities::AuthoredToolId,
                    SearchEntities::Fingerprint,
                ])
                .from(SearchEntities::Table)
                .and_where(Expr::col(SearchEntities::Kind).is_in(AUTHORED_TOOL_KINDS))
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(kind, workspace_id, id, fingerprint)| {
            Ok(RecordedAuthoredTool {
                tool: workspace_authored_tool(&kind, &workspace_id, &id)?,
                fingerprint,
            })
        })
        .collect()
}

/// Reads the fingerprint of one Authored Tool's row, `None` when it has no
/// row.
pub(super) async fn recorded_authored_tool_fingerprint(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    id: &AuthoredToolId,
) -> Result<Option<String>, IndexError> {
    let row: Option<(String,)> = transaction
        .fetch_optional(
            Query::select()
                .column(SearchEntities::Fingerprint)
                .from(SearchEntities::Table)
                .and_where(Expr::col(SearchEntities::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(SearchEntities::Kind).eq(authored_tool(id.kind())))
                .and_where(Expr::col(SearchEntities::DocId).eq(id.to_string()))
                .to_owned(),
        )
        .await?;
    Ok(row.map(|(fingerprint,)| fingerprint))
}

/// One Authored Tool whose last attempt failed, as a reconcile pass
/// compares it.
pub(super) struct FailedAuthoredTool {
    pub(super) tool: WorkspaceAuthoredTool,
    pub(super) failure: AuthoredToolFailure,
}

#[derive(sqlx::FromRow)]
struct FailedAuthoredToolRow {
    kind: String,
    workspace_id: String,
    authored_tool_id: String,
    #[sqlx(flatten)]
    failure: FailedStateRow,
}

/// Reads every Authored Tool, of either kind, whose last attempt failed.
pub(super) async fn failed_authored_tools(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<FailedAuthoredTool>, IndexError> {
    let rows: Vec<FailedAuthoredToolRow> = transaction
        .fetch_all(
            Query::select()
                .columns(with_retry_columns([
                    SearchIndexStates::Kind,
                    SearchIndexStates::WorkspaceId,
                    SearchIndexStates::AuthoredToolId,
                    SearchIndexStates::FailedAuthoredToolRevision,
                ]))
                .from(SearchIndexStates::Table)
                .and_where(Expr::col(SearchIndexStates::Kind).is_in(AUTHORED_TOOL_KINDS))
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|row| {
            Ok(FailedAuthoredTool {
                tool: workspace_authored_tool(&row.kind, &row.workspace_id, &row.authored_tool_id)?,
                failure: row.failure.into(),
            })
        })
        .collect()
}

/// What a statistics count saw of one Workspace's rows: how many, and the
/// newest `indexed_at_unix_nanos`, `None` when there were none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Watermark {
    rows: i64,
    max_indexed_at_unix_nanos: Option<i64>,
}

/// A Workspace id, its stored watermark's row count and newest stamp, and
/// its rows' count and newest stamp now.
type WatermarkColumns = (String, Option<i64>, Option<i64>, i64, Option<i64>);

/// One Workspace's statistics watermark beside its rows now.
pub(super) struct WorkspaceWatermarks {
    pub(super) workspace_id: WorkspaceId,
    /// The watermark the last count stored; `None` when no count stored one.
    pub(super) counted: Option<Watermark>,
    /// The watermark of the Workspace's rows now.
    pub(super) current: Watermark,
}

/// On Postgres, reads the watermarks of every Workspace that has rows or
/// statistics, each once: a Workspace whose last row went still has
/// statistics to rewrite. `SQLite` keeps no statistics of its own, so it has
/// none.
pub(super) async fn statistics_watermarks(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<WorkspaceWatermarks>, IndexError> {
    if transaction.system() != DatabaseSystem::Postgres {
        return Ok(Vec::new());
    }
    let workspace_rows = Alias::new("workspace_rows");
    let row_count = Alias::new("row_count");
    let max_indexed_at = Alias::new("max_indexed_at");
    let rows: Vec<WatermarkColumns> = transaction
        .fetch_all(
            Query::select()
                .expr(Func::coalesce([
                    Expr::col((workspace_rows.clone(), SearchEntities::WorkspaceId)),
                    Expr::col((SearchStats::Table, SearchStats::WorkspaceId)),
                ]))
                .column((SearchStats::Table, SearchStats::CountedRows))
                .column((SearchStats::Table, SearchStats::CountedMaxIndexedAt))
                .expr(Func::coalesce([
                    Expr::col((workspace_rows.clone(), row_count.clone())),
                    Expr::val(0_i64),
                ]))
                .column((workspace_rows.clone(), max_indexed_at.clone()))
                .from_subquery(
                    Query::select()
                        .column(SearchEntities::WorkspaceId)
                        .expr_as(Expr::col(Asterisk).count(), row_count)
                        .expr_as(
                            Expr::col(SearchEntities::IndexedAtUnixNanos).max(),
                            max_indexed_at,
                        )
                        .from(SearchEntities::Table)
                        .group_by_col(SearchEntities::WorkspaceId)
                        .to_owned(),
                    workspace_rows.clone(),
                )
                .join(
                    JoinType::FullOuterJoin,
                    SearchStats::Table,
                    Expr::col((SearchStats::Table, SearchStats::WorkspaceId))
                        .equals((workspace_rows, SearchEntities::WorkspaceId)),
                )
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(
            |(workspace_id, counted_rows, counted_max, rows, max_indexed_at_unix_nanos)| {
                Ok(WorkspaceWatermarks {
                    workspace_id: parse_workspace_id(&workspace_id)?,
                    counted: counted_rows.map(|rows| Watermark {
                        rows,
                        max_indexed_at_unix_nanos: counted_max,
                    }),
                    current: Watermark {
                        rows,
                        max_indexed_at_unix_nanos,
                    },
                })
            },
        )
        .collect()
}

/// One Workspace's stored watermark's row count and newest stamp, and its
/// rows' count and newest stamp now.
type WorkspaceWatermarkColumns = (Option<i64>, Option<i64>, i64, Option<i64>);

/// On Postgres, reads one Workspace's statistics watermark beside its rows
/// now. `None` on `SQLite`, which keeps no statistics of its own.
pub(super) async fn workspace_watermark(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<Option<WorkspaceWatermarks>, IndexError> {
    if transaction.system() != DatabaseSystem::Postgres {
        return Ok(None);
    }
    let rows: Vec<WorkspaceWatermarkColumns> = transaction
        .fetch_all_sql(
            "SELECT s.counted_rows, s.counted_max_indexed_at, r.row_count, r.max_indexed_at \
             FROM ( \
                 SELECT count(*) AS row_count, max(indexed_at_unix_nanos) AS max_indexed_at \
                 FROM search_entities WHERE workspace_id = $1 \
             ) AS r \
             LEFT JOIN search_stats AS s ON s.workspace_id = $1",
            sea_query::Values(vec![workspace_id.to_string().into()]),
        )
        .await?;
    let (counted_rows, counted_max, rows, max_indexed_at_unix_nanos) = rows
        .into_iter()
        .next()
        // An aggregate without a grouping always returns one row.
        .ok_or_else(|| Error::from(sqlx::Error::RowNotFound))?;
    Ok(Some(WorkspaceWatermarks {
        workspace_id: workspace_id.clone(),
        counted: counted_rows.map(|rows| Watermark {
            rows,
            max_indexed_at_unix_nanos: counted_max,
        }),
        current: Watermark {
            rows,
            max_indexed_at_unix_nanos,
        },
    }))
}

fn workspace_generation(
    workspace_id: &str,
    generation_id: &str,
) -> Result<WorkspaceGeneration, IndexError> {
    Ok(WorkspaceGeneration {
        workspace_id: parse_workspace_id(workspace_id)?,
        generation_id: ProviderGenerationId::parse(generation_id).map_err(|_error| {
            IndexError::Corrupt("a search index row has an invalid Generation id")
        })?,
    })
}

fn workspace_authored_tool(
    kind: &str,
    workspace_id: &str,
    id: &str,
) -> Result<WorkspaceAuthoredTool, IndexError> {
    let kind = [AuthoredToolKind::Private, AuthoredToolKind::Shared]
        .into_iter()
        .find(|authored| authored_tool(*authored) == kind)
        .ok_or(IndexError::Corrupt(
            "a search index row of an Authored Tool has another kind",
        ))?;
    Ok(WorkspaceAuthoredTool {
        workspace_id: parse_workspace_id(workspace_id)?,
        id: AuthoredToolId::parse(kind, id).map_err(|_error| {
            IndexError::Corrupt("a search index row has an invalid Authored Tool id")
        })?,
    })
}

fn parse_workspace_id(value: &str) -> Result<WorkspaceId, IndexError> {
    WorkspaceId::parse(value)
        .map_err(|_error| IndexError::Corrupt("a search index row has an invalid Workspace id"))
}

/// On Postgres, rewrites a Workspace's ranking statistics from its rows: the
/// document frequency of every lexeme, the document count, and the mean
/// vector length. `SQLite`'s FTS5 keeps its own statistics. The caller
/// holds the Workspace's statistics lock.
///
/// One statement counts and writes, so the counts and the watermark beside
/// them come from one snapshot: `ts_stat` reads its query with the snapshot
/// of the statement that calls it. The statement writes only what changed:
/// it updates the terms whose count changed, inserts the new terms, deletes
/// the terms that are gone, and updates the totals row only when a value in
/// it changed. Returns the rows written or deleted.
pub(super) async fn recount_workspace_statistics(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<u64, Error> {
    let workspace_id = workspace_id.to_string();
    if transaction.system() != DatabaseSystem::Postgres {
        return Ok(0);
    }
    // `ts_stat` takes query text, so the Workspace id enters it quoted as a
    // literal by `format('%L')`; the value itself still travels as a bind.
    //
    // The three term sub-statements touch disjoint rows: one statement must
    // not write one row twice. The terms are not an upsert with a `WHERE`
    // on its update: that locks each term row it matches and logs it, also
    // when the count did not change. Under the Workspace's statistics lock
    // no other recount inserts a term of this Workspace, so the insert meets
    // no conflict. A process of an earlier release counts inside its index
    // write under the write lock instead; during a rolling update its count
    // may meet this one, fail, and leave the watermark stale for the next
    // pass. The totals row is one row, so its upsert may skip by `WHERE`.
    let (written,): (i64,) = transaction
        .fetch_all_sql(
            "WITH counted AS MATERIALIZED ( \
                 SELECT word AS term, ndoc \
                 FROM ts_stat(format( \
                     'SELECT tsv FROM search_entities WHERE workspace_id = %L', $1::text \
                 )) \
             ), \
             changed AS ( \
                 UPDATE search_terms AS t SET ndoc = c.ndoc \
                 FROM counted AS c \
                 WHERE t.workspace_id = $1::text AND t.term = c.term AND t.ndoc <> c.ndoc \
                 RETURNING 1 \
             ), \
             added AS ( \
                 INSERT INTO search_terms (workspace_id, term, ndoc) \
                 SELECT $1::text, c.term, c.ndoc FROM counted AS c \
                 WHERE NOT EXISTS ( \
                     SELECT 1 FROM search_terms AS t \
                     WHERE t.workspace_id = $1::text AND t.term = c.term \
                 ) \
                 RETURNING 1 \
             ), \
             gone AS ( \
                 DELETE FROM search_terms AS t \
                 WHERE t.workspace_id = $1::text \
                   AND NOT EXISTS (SELECT 1 FROM counted AS c WHERE c.term = t.term) \
                 RETURNING 1 \
             ), \
             totals AS ( \
                 INSERT INTO search_stats AS s ( \
                     workspace_id, total_docs, avgdl, counted_rows, counted_max_indexed_at \
                 ) \
                 SELECT $1::text, count(*), coalesce(avg(tsv_length), 1.0), \
                     count(*), max(indexed_at_unix_nanos) \
                 FROM search_entities WHERE workspace_id = $1::text \
                 ON CONFLICT (workspace_id) DO UPDATE \
                 SET total_docs = excluded.total_docs, avgdl = excluded.avgdl, \
                     counted_rows = excluded.counted_rows, \
                     counted_max_indexed_at = excluded.counted_max_indexed_at \
                 WHERE (s.total_docs, s.avgdl, s.counted_rows, s.counted_max_indexed_at) \
                     IS DISTINCT FROM (excluded.total_docs, excluded.avgdl, \
                         excluded.counted_rows, excluded.counted_max_indexed_at) \
                 RETURNING 1 \
             ) \
             SELECT (SELECT count(*) FROM changed) + (SELECT count(*) FROM added) \
                 + (SELECT count(*) FROM gone) + (SELECT count(*) FROM totals)",
            sea_query::Values(vec![workspace_id.into()]),
        )
        .await?
        .into_iter()
        .next()
        .ok_or(sqlx::Error::RowNotFound)?;
    // A sum of counts is never negative.
    Ok(written.unsigned_abs())
}

#[cfg(test)]
mod failure_kind_tests {
    use super::{FAILURES, failure_kind, parse_failure_kind};
    use crate::SearchIndexFailure;

    // Catches: a kind the write records but the read cannot name, which
    // would show "No reason recorded." instead of the real reason.
    #[test]
    fn every_failure_kind_reads_back_as_itself() {
        let every_kind = [
            SearchIndexFailure::InvalidEntry,
            SearchIndexFailure::TimedOut,
            SearchIndexFailure::Clock,
            SearchIndexFailure::Corrupt,
            SearchIndexFailure::Provider,
            SearchIndexFailure::Database,
        ];
        for kind in every_kind {
            // Exhaustive: a new kind stops this compiling until it is listed.
            match kind {
                SearchIndexFailure::InvalidEntry
                | SearchIndexFailure::TimedOut
                | SearchIndexFailure::Clock
                | SearchIndexFailure::Corrupt
                | SearchIndexFailure::Provider
                | SearchIndexFailure::Database => {}
            }
            assert!(FAILURES.contains(&kind), "{kind:?} is readable");
            assert_eq!(parse_failure_kind(failure_kind(kind)), Some(kind));
        }
        let values = FAILURES
            .map(failure_kind)
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(values.len(), FAILURES.len(), "stored values are distinct");
    }
}
