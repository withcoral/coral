//! The indexers' statements over the search-owned tables.
//!
//! Every function joins the caller's transaction; [`crate::SearchIndex`] owns
//! when each transaction begins, commits, or rolls back.

use std::collections::{HashMap, HashSet};

use coral_catalog::{ProviderGenerationId, ToolKey};
use coral_db::{DatabaseSystem, Error, Transaction};
use coral_functions::SavedFunctionId;
use coral_provider::ProviderId;
use coral_workspace::{Principal, WorkspaceId};
use sea_query::{
    Alias, Asterisk, BinOper, Cond, DeleteStatement, Expr, ExprTrait as _, Func, Iden, JoinType,
    OnConflict, Query, SimpleExpr,
};

use super::retry::RetryRule;
use super::{INDEXER_REVISION, IndexError};
use crate::{CallPath, SearchIndexFailure, SearchIndexState};

/// The `search_entities` table and its columns, for sea-query. Each row is
/// one Search Entity that indexed: a Tool or a Saved Function.
#[derive(Clone, Copy, Iden)]
enum SearchEntities {
    Table,
    WorkspaceId,
    DocId,
    Kind,
    ProviderId,
    ProviderGenerationId,
    ToolKey,
    SavedFunctionId,
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
    /// The row's vector length, on Postgres only. A write that changes the
    /// row clears it, and the indexer then counts it again.
    DocLen,
}

/// The `search_index_states` table and its columns, for sea-query. Each row
/// is the state of one indexing attempt: one per Generation attempt, one per
/// failed Saved Function attempt.
#[derive(Clone, Copy, Iden)]
enum SearchIndexStates {
    Table,
    WorkspaceId,
    TargetId,
    Kind,
    ProviderId,
    ProviderGenerationId,
    ProviderDisplayName,
    SavedFunctionId,
    PrincipalId,
    PrincipalKind,
    State,
    AttemptedAtUnixNanos,
    Error,
    /// The kind of error a failed Generation attempt recorded.
    ErrorKind,
    /// The indexer revision that wrote a Generation's state and rows.
    IndexerRevision,
    /// The columns of a failed attempt's [`RetryRule`], [`RETRY_COLUMNS`].
    FailedAttempts,
    RetryAfterUnixNanos,
    FailedCoralVersion,
    /// The definition revision a failed Saved Function attempt tried.
    FailedSavedFunctionRevision,
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
const SAVED_FUNCTION: &str = "saved_function";
const COMPLETE: &str = "complete";
const FAILED: &str = "failed";

/// Rows per upsert or delete statement, well under each backend's bind
/// limit.
const CHUNK_ROWS: usize = 500;

/// The class of the Postgres advisory lock on one Workspace's search index
/// writes, ASCII `srch`. The second key is the hash of the Workspace id. The
/// two-key form never conflicts with the one-key lock of the stream steps.
const WORKSPACE_LOCK_CLASS: i32 = 0x7372_6368;

/// On Postgres, takes the transaction-scoped lock on the Workspace's search
/// index writes, so two processes that write one Workspace take turns.
/// `Database::begin` runs at the server's default isolation, read committed,
/// so each later statement of the transaction sees what the previous holder
/// committed, and its statistics count includes those rows. `SQLite`
/// serializes every write already.
pub(super) async fn lock_workspace(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<(), Error> {
    lock_workspace_id(transaction, &workspace_id.to_string()).await
}

async fn lock_workspace_id(
    transaction: &mut Transaction<'_>,
    workspace_id: &str,
) -> Result<(), Error> {
    if transaction.system() != DatabaseSystem::Postgres {
        return Ok(());
    }
    transaction
        .execute_sql(
            "SELECT pg_advisory_xact_lock($1, hashtext($2))",
            sea_query::Values(vec![WORKSPACE_LOCK_CLASS.into(), workspace_id.into()]),
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
        statement.on_conflict(changed_rows_only(transaction.system(), TOOL_COLUMNS));
        transaction.execute(statement).await?;
    }
    Ok(())
}

/// The conflict action of a row upsert: it updates a conflicting row only
/// when the insert brings another value for a column other than the key and
/// the stamp. A row whose values stay the same keeps its stamp and its
/// version, and no trigger fires for it. On Postgres the update also clears
/// `doc_len`, so the length refresh counts that row again.
///
/// Both databases compare with `IS DISTINCT FROM`, so two NULLs are equal.
/// `SQLite` has it from 3.39; the bundled build is newer.
///
/// `columns` opens with the two key columns and ends with the stamp.
fn changed_rows_only<const N: usize>(
    system: DatabaseSystem,
    columns: [SearchEntities; N],
) -> OnConflict {
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
    if system == DatabaseSystem::Postgres {
        on_conflict.value(SearchEntities::DocLen, Expr::null());
    }
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

/// Deletes every Tool row of the Generation, then refreshes the
/// Workspace's statistics.
pub(super) async fn delete_generation_rows(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    generation_id: &ProviderGenerationId,
) -> Result<(), Error> {
    transaction
        .execute(generation_rows(workspace_id, generation_id))
        .await?;
    refresh_statistics(transaction, &workspace_id.to_string()).await?;
    Ok(())
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
                        .to_owned(),
                )
                .to_owned(),
        )
        .await?;
    Ok(())
}

/// Deletes one Generation's Tool rows and state row, then refreshes the
/// statistics of the Workspace its state row named, under that Workspace's
/// lock. Nothing is locked or refreshed when the index held no state for the
/// Generation: the next reconcile pass sees the changed rows.
pub(super) async fn delete_generation(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
) -> Result<(), Error> {
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
    if let Some((workspace_id,)) = &workspace {
        lock_workspace_id(transaction, workspace_id).await?;
    }
    transaction
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
    if let Some((workspace_id,)) = workspace {
        refresh_statistics(transaction, &workspace_id).await?;
    }
    Ok(())
}

/// Deletes the Tool rows and state rows of every Generation of one
/// Provider, then refreshes the Workspace's statistics.
pub(super) async fn delete_provider(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider_id: &ProviderId,
) -> Result<(), Error> {
    let workspace_id = workspace_id.to_string();
    let provider_id = provider_id.to_string();
    transaction
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
    refresh_statistics(transaction, &workspace_id).await?;
    Ok(())
}

/// The columns a Saved Function row sets, in [`FunctionRow::values`] order.
/// The first two are the key.
const FUNCTION_COLUMNS: [SearchEntities; 15] = [
    SearchEntities::WorkspaceId,
    SearchEntities::DocId,
    SearchEntities::Kind,
    SearchEntities::SavedFunctionId,
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

/// One Saved Function's row in `search_entities`. The document id is the
/// function id; the fingerprint is [`function_fingerprint`] of the
/// definition revision. The write stamps the row when it upserts it.
pub(crate) struct FunctionRow<'a> {
    pub(super) workspace_id: &'a WorkspaceId,
    pub(super) function_id: &'a SavedFunctionId,
    pub(super) principal: &'a Principal,
    pub(super) call_path: CallPath,
    pub(super) qualified_name: &'a str,
    pub(super) description: &'a str,
    pub(super) searchable_text: String,
    pub(super) revision: u64,
}

impl FunctionRow<'_> {
    fn values(&self, indexed_at_unix_nanos: i64) -> [SimpleExpr; FUNCTION_COLUMNS.len()] {
        let function_id = self.function_id.to_string();
        [
            self.workspace_id.to_string().into(),
            function_id.clone().into(),
            SAVED_FUNCTION.into(),
            function_id.into(),
            self.principal.id().as_str().into(),
            self.principal.kind().as_storage_str().into(),
            self.call_path.as_str().into(),
            self.qualified_name.into(),
            "".into(),
            self.description.into(),
            "".into(),
            self.searchable_text.clone().into(),
            false.into(),
            function_fingerprint(self.revision).into(),
            indexed_at_unix_nanos.into(),
        ]
    }
}

/// The fingerprint of a Saved Function row: the definition revision and the
/// indexer revision, as `3.1`. A row that an older indexer revision wrote
/// holds another fingerprint, so a reconcile pass indexes it again. A Coral
/// without indexer revisions wrote the definition revision alone.
pub(super) fn function_fingerprint(revision: u64) -> String {
    format!("{revision}.{INDEXER_REVISION}")
}

/// One failed Saved Function attempt's row in `search_index_states`.
pub(super) struct FailedFunctionState<'a> {
    pub(super) workspace_id: &'a WorkspaceId,
    pub(super) function_id: &'a SavedFunctionId,
    pub(super) principal: &'a Principal,
    pub(super) attempted_at_unix_nanos: i64,
    pub(super) error: String,
    pub(super) retry: RetryRule,
    /// The definition revision the attempt tried.
    pub(super) revision: u64,
}

/// Upserts a Saved Function row by Workspace and document id, stamped with
/// `indexed_at_unix_nanos`, as [`changed_rows_only`] describes. An update,
/// never `INSERT OR REPLACE`, for the reason [`upsert_tool_rows`]
/// gives.
pub(super) async fn upsert_function_row(
    transaction: &mut Transaction<'_>,
    row: &FunctionRow<'_>,
    indexed_at_unix_nanos: i64,
) -> Result<(), Error> {
    let on_conflict = changed_rows_only(transaction.system(), FUNCTION_COLUMNS);
    transaction
        .execute(
            Query::insert()
                .into_table(SearchEntities::Table)
                .columns(FUNCTION_COLUMNS)
                .values_panic(row.values(indexed_at_unix_nanos))
                .on_conflict(on_conflict)
                .to_owned(),
        )
        .await?;
    Ok(())
}

/// Deletes a Saved Function's row, then refreshes the Workspace's statistics.
pub(super) async fn delete_function_row(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    function_id: &SavedFunctionId,
) -> Result<(), Error> {
    let workspace_id = workspace_id.to_string();
    transaction
        .execute(
            Query::delete()
                .from_table(SearchEntities::Table)
                .and_where(Expr::col(SearchEntities::WorkspaceId).eq(workspace_id.as_str()))
                .and_where(Expr::col(SearchEntities::DocId).eq(function_id.to_string()))
                .and_where(Expr::col(SearchEntities::Kind).eq(SAVED_FUNCTION))
                .to_owned(),
        )
        .await?;
    refresh_statistics(transaction, &workspace_id).await?;
    Ok(())
}

/// Upserts a Saved Function's `failed` state row.
pub(super) async fn write_failed_function_state(
    transaction: &mut Transaction<'_>,
    state: FailedFunctionState<'_>,
) -> Result<(), Error> {
    let function_id = state.function_id.to_string();
    transaction
        .execute(
            Query::insert()
                .into_table(SearchIndexStates::Table)
                .columns(with_retry_columns([
                    SearchIndexStates::WorkspaceId,
                    SearchIndexStates::TargetId,
                    SearchIndexStates::Kind,
                    SearchIndexStates::SavedFunctionId,
                    SearchIndexStates::PrincipalId,
                    SearchIndexStates::PrincipalKind,
                    SearchIndexStates::State,
                    SearchIndexStates::AttemptedAtUnixNanos,
                    SearchIndexStates::Error,
                    SearchIndexStates::FailedSavedFunctionRevision,
                ]))
                .values_panic(
                    [
                        state.workspace_id.to_string().into(),
                        function_id.clone().into(),
                        SAVED_FUNCTION.into(),
                        function_id.into(),
                        state.principal.id().as_str().into(),
                        state.principal.kind().as_storage_str().into(),
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
                            SearchIndexStates::FailedSavedFunctionRevision,
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

/// A definition revision as its `BIGINT` column holds it. A revision past
/// the column's range is stored as `NULL`, which matches no revision.
fn revision_value(revision: u64) -> SimpleExpr {
    i64::try_from(revision).ok().into()
}

/// What a target's `failed` state row records for its next attempt.
#[derive(sqlx::FromRow)]
struct FailedStateRow {
    failed_saved_function_revision: Option<i64>,
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
                    SearchIndexStates::FailedSavedFunctionRevision,
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

/// The failure a Saved Function's state row records.
pub(super) struct FunctionFailure {
    pub(super) retry: RetryRule,
    /// The definition revision the attempt tried. `None` on a row written
    /// before Coral recorded it.
    pub(super) revision: Option<u64>,
}

impl From<FailedStateRow> for FunctionFailure {
    fn from(row: FailedStateRow) -> Self {
        Self {
            retry: row.retry.into(),
            revision: row
                .failed_saved_function_revision
                .and_then(|revision| u64::try_from(revision).ok()),
        }
    }
}

/// Reads the failure of the Saved Function's state row, when its last
/// attempt failed.
pub(super) async fn failed_function(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    function_id: &SavedFunctionId,
) -> Result<Option<FunctionFailure>, Error> {
    let row = failed_state(
        transaction,
        workspace_id,
        SAVED_FUNCTION,
        function_id.to_string(),
    )
    .await?;
    Ok(row.map(FunctionFailure::from))
}

/// Deletes a Saved Function's state row, if an earlier attempt left one.
pub(super) async fn delete_function_state(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    function_id: &SavedFunctionId,
) -> Result<(), Error> {
    transaction
        .execute(
            Query::delete()
                .from_table(SearchIndexStates::Table)
                .and_where(Expr::col(SearchIndexStates::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(SearchIndexStates::Kind).eq(SAVED_FUNCTION))
                .and_where(Expr::col(SearchIndexStates::TargetId).eq(function_id.to_string()))
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
    #[sqlx(flatten)]
    retry: RetryColumns,
}

/// Reads every Generation's state row in every Workspace.
pub(super) async fn recorded_generations(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<RecordedGeneration>, IndexError> {
    let rows: Vec<RecordedGenerationRow> = transaction
        .fetch_all(
            Query::select()
                .columns(with_retry_columns([
                    SearchIndexStates::WorkspaceId,
                    SearchIndexStates::ProviderGenerationId,
                    SearchIndexStates::State,
                    SearchIndexStates::ProviderDisplayName,
                    SearchIndexStates::IndexerRevision,
                ]))
                .from(SearchIndexStates::Table)
                .and_where(Expr::col(SearchIndexStates::Kind).eq(TOOL))
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|row| {
            // The state check admits only `complete` and `failed`.
            let outcome = if row.state == COMPLETE {
                RecordedOutcome::Complete {
                    display_name: row.provider_display_name,
                }
            } else {
                RecordedOutcome::Failed {
                    retry: row.retry.into(),
                }
            };
            Ok(RecordedGeneration {
                generation: workspace_generation(&row.workspace_id, &row.provider_generation_id)?,
                outcome,
                indexer_revision: row.indexer_revision,
            })
        })
        .collect()
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
/// Workspace. The caller refreshes the Workspace's statistics.
pub(super) async fn delete_generation_in_workspace(
    transaction: &mut Transaction<'_>,
    generation: &WorkspaceGeneration,
) -> Result<(), Error> {
    transaction
        .execute(generation_rows(
            &generation.workspace_id,
            &generation.generation_id,
        ))
        .await?;
    transaction
        .execute(
            Query::delete()
                .from_table(SearchIndexStates::Table)
                .and_where(
                    Expr::col(SearchIndexStates::WorkspaceId)
                        .eq(generation.workspace_id.to_string()),
                )
                .and_where(Expr::col(SearchIndexStates::Kind).eq(TOOL))
                .and_where(
                    Expr::col(SearchIndexStates::ProviderGenerationId)
                        .eq(generation.generation_id.to_string()),
                )
                .to_owned(),
        )
        .await?;
    Ok(())
}

/// One Saved Function in its Workspace.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct WorkspaceFunction {
    pub(super) workspace_id: WorkspaceId,
    pub(super) function_id: SavedFunctionId,
}

/// One Saved Function's row, as a reconcile pass compares it.
pub(super) struct RecordedFunction {
    pub(super) function: WorkspaceFunction,
    /// The row's fingerprint, as [`function_fingerprint`] writes it.
    pub(super) fingerprint: String,
}

/// Reads every Saved Function row in every Workspace.
pub(super) async fn recorded_functions(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<RecordedFunction>, IndexError> {
    let rows: Vec<(String, String, String)> = transaction
        .fetch_all(
            Query::select()
                .columns([
                    SearchEntities::WorkspaceId,
                    SearchEntities::SavedFunctionId,
                    SearchEntities::Fingerprint,
                ])
                .from(SearchEntities::Table)
                .and_where(Expr::col(SearchEntities::Kind).eq(SAVED_FUNCTION))
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(workspace_id, function_id, fingerprint)| {
            Ok(RecordedFunction {
                function: workspace_function(&workspace_id, &function_id)?,
                fingerprint,
            })
        })
        .collect()
}

/// One Saved Function whose last attempt failed, as a reconcile pass
/// compares it.
pub(super) struct FailedFunction {
    pub(super) function: WorkspaceFunction,
    pub(super) failure: FunctionFailure,
}

#[derive(sqlx::FromRow)]
struct FailedFunctionRow {
    workspace_id: String,
    saved_function_id: String,
    #[sqlx(flatten)]
    failure: FailedStateRow,
}

/// Reads every Saved Function whose last attempt failed.
pub(super) async fn failed_functions(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<FailedFunction>, IndexError> {
    let rows: Vec<FailedFunctionRow> = transaction
        .fetch_all(
            Query::select()
                .columns(with_retry_columns([
                    SearchIndexStates::WorkspaceId,
                    SearchIndexStates::SavedFunctionId,
                    SearchIndexStates::FailedSavedFunctionRevision,
                ]))
                .from(SearchIndexStates::Table)
                .and_where(Expr::col(SearchIndexStates::Kind).eq(SAVED_FUNCTION))
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|row| {
            Ok(FailedFunction {
                function: workspace_function(&row.workspace_id, &row.saved_function_id)?,
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

fn workspace_function(
    workspace_id: &str,
    function_id: &str,
) -> Result<WorkspaceFunction, IndexError> {
    Ok(WorkspaceFunction {
        workspace_id: parse_workspace_id(workspace_id)?,
        function_id: SavedFunctionId::parse(function_id).map_err(|_error| {
            IndexError::Corrupt("a search index row has an invalid Saved Function id")
        })?,
    })
}

fn parse_workspace_id(value: &str) -> Result<WorkspaceId, IndexError> {
    WorkspaceId::parse(value)
        .map_err(|_error| IndexError::Corrupt("a search index row has an invalid Workspace id"))
}

/// On Postgres, counts `doc_len` for each of one Generation's rows that has
/// none: the number of lexeme positions in the row's vector. An insert and
/// an update leave the length empty; a row the write kept keeps its length.
/// `SQLite` needs none.
pub(super) async fn refresh_generation_lengths(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    generation_id: &ProviderGenerationId,
) -> Result<(), Error> {
    if transaction.system() != DatabaseSystem::Postgres {
        return Ok(());
    }
    transaction
        .execute_sql(
            "UPDATE search_entities AS d \
             SET doc_len = coalesce(( \
                 SELECT sum(coalesce(array_length(u.positions, 1), 1)) \
                 FROM unnest(d.tsv) AS u(lexeme, positions, weights) \
             ), 0) \
             WHERE d.workspace_id = $1 AND d.provider_generation_id = $2 \
               AND d.doc_len IS NULL",
            sea_query::Values(vec![
                workspace_id.to_string().into(),
                generation_id.to_string().into(),
            ]),
        )
        .await?;
    Ok(())
}

/// On Postgres, counts `doc_len` for one Saved Function's row when it has
/// none, as [`refresh_generation_lengths`] does for a Generation's.
pub(super) async fn refresh_function_length(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    function_id: &SavedFunctionId,
) -> Result<(), Error> {
    if transaction.system() != DatabaseSystem::Postgres {
        return Ok(());
    }
    transaction
        .execute_sql(
            "UPDATE search_entities AS d \
             SET doc_len = coalesce(( \
                 SELECT sum(coalesce(array_length(u.positions, 1), 1)) \
                 FROM unnest(d.tsv) AS u(lexeme, positions, weights) \
             ), 0) \
             WHERE d.workspace_id = $1 AND d.saved_function_id = $2 \
               AND d.doc_len IS NULL",
            sea_query::Values(vec![
                workspace_id.to_string().into(),
                function_id.to_string().into(),
            ]),
        )
        .await?;
    Ok(())
}

/// On Postgres, rewrites a Workspace's ranking statistics from its rows: the
/// document frequency of every lexeme, the document count, and the mean
/// document length. `SQLite`'s FTS5 keeps its own statistics. The caller
/// holds the Workspace's lock.
///
/// One statement counts and writes, so the counts and the watermark beside
/// them come from one snapshot: `ts_stat` reads its query with the snapshot
/// of the statement that calls it. The statement writes only what changed:
/// it updates the terms whose count changed, inserts the new terms, deletes
/// the terms that are gone, and updates the totals row only when a value in
/// it changed. Returns the rows written or deleted.
pub(super) async fn refresh_workspace_statistics(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<u64, Error> {
    refresh_statistics(transaction, &workspace_id.to_string()).await
}

async fn refresh_statistics(
    transaction: &mut Transaction<'_>,
    workspace_id: &str,
) -> Result<u64, Error> {
    if transaction.system() != DatabaseSystem::Postgres {
        return Ok(0);
    }
    // `ts_stat` takes query text, so the Workspace id enters it quoted as a
    // literal by `format('%L')`; the value itself still travels as a bind.
    //
    // The three term sub-statements touch disjoint rows: one statement must
    // not write one row twice. The terms are not an upsert with a `WHERE`
    // on its update: that locks each term row it matches and logs it, also
    // when the count did not change. Under the Workspace's lock no other
    // process inserts a term of this Workspace, so the insert meets no
    // conflict. The totals row is one row, so its upsert may skip by `WHERE`.
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
                 SELECT $1::text, count(*), coalesce(avg(doc_len), 1.0), \
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
