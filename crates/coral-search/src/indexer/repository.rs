//! The indexers' statements over the search-owned tables.
//!
//! Every function joins the caller's transaction; [`crate::SearchIndex`] owns
//! when each transaction begins, commits, or rolls back.

use coral_catalog::{OperationKey, ProviderGenerationId};
use coral_db::{DatabaseSystem, Error, Transaction};
use coral_functions::SavedFunctionId;
use coral_provider::ProviderId;
use coral_workspace::{Principal, WorkspaceId};
use sea_query::{DeleteStatement, Expr, ExprTrait as _, Iden, OnConflict, Query, SimpleExpr};

use super::IndexError;
use crate::CallPath;

/// The `search_entities` table and its columns, for sea-query. Each row is
/// one Search Entity that indexed: an Operation or a Saved Function.
#[derive(Iden)]
enum SearchEntities {
    Table,
    WorkspaceId,
    DocId,
    Kind,
    ProviderId,
    ProviderGenerationId,
    OperationKey,
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
    IndexedAtUnixNanos,
}

/// The `search_index_states` table and its columns, for sea-query. Each row
/// is the state of one indexing attempt: one per Generation attempt, one per
/// failed Saved Function attempt.
#[derive(Iden)]
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
}

const OPERATION: &str = "operation";
const SAVED_FUNCTION: &str = "saved_function";
const COMPLETE: &str = "complete";
const FAILED: &str = "failed";

/// Rows per upsert statement, well under each backend's bind limit.
const UPSERT_CHUNK_ROWS: usize = 500;

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

/// The columns an Operation row sets, in [`OperationRow::values`] order. The
/// first two are the key.
const OPERATION_COLUMNS: [SearchEntities; 15] = [
    SearchEntities::WorkspaceId,
    SearchEntities::DocId,
    SearchEntities::Kind,
    SearchEntities::ProviderId,
    SearchEntities::ProviderGenerationId,
    SearchEntities::OperationKey,
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

/// One Operation's row in `search_entities`. The document id and the
/// fingerprint derive from the Generation and the Operation Key.
pub(super) struct OperationRow<'a> {
    pub(super) workspace_id: &'a WorkspaceId,
    pub(super) provider_id: &'a ProviderId,
    pub(super) generation_id: &'a ProviderGenerationId,
    pub(super) operation_key: &'a OperationKey,
    pub(super) call_path: CallPath,
    pub(super) qualified_name: &'a str,
    pub(super) title: String,
    pub(super) description: String,
    pub(super) provider_name: String,
    pub(super) searchable_text: String,
    pub(super) deprecated: bool,
    pub(super) indexed_at_unix_nanos: i64,
}

impl OperationRow<'_> {
    fn values(&self) -> [SimpleExpr; OPERATION_COLUMNS.len()] {
        let generation_id = self.generation_id.to_string();
        [
            self.workspace_id.to_string().into(),
            format!("{generation_id}:{}", self.operation_key.as_str()).into(),
            OPERATION.into(),
            self.provider_id.to_string().into(),
            generation_id.clone().into(),
            self.operation_key.as_str().into(),
            self.call_path.as_str().into(),
            self.qualified_name.into(),
            self.title.clone().into(),
            self.description.clone().into(),
            self.provider_name.clone().into(),
            self.searchable_text.clone().into(),
            self.deprecated.into(),
            generation_id.into(),
            self.indexed_at_unix_nanos.into(),
        ]
    }
}

/// The outcome an index attempt records.
pub(super) enum AttemptState {
    Complete,
    Failed { error: String },
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

/// Upserts Operation rows by Workspace and document id. An update, never
/// `INSERT OR REPLACE`: on `SQLite` a replace skips the delete trigger and
/// leaves a stale copy in the FTS5 table.
pub(super) async fn upsert_operation_rows(
    transaction: &mut Transaction<'_>,
    rows: &[OperationRow<'_>],
) -> Result<(), Error> {
    for chunk in rows.chunks(UPSERT_CHUNK_ROWS) {
        let mut statement = Query::insert();
        statement
            .into_table(SearchEntities::Table)
            .columns(OPERATION_COLUMNS);
        for row in chunk {
            statement.values_panic(row.values());
        }
        statement.on_conflict(
            OnConflict::columns([SearchEntities::WorkspaceId, SearchEntities::DocId])
                .update_columns(OPERATION_COLUMNS.into_iter().skip(2))
                .to_owned(),
        );
        transaction.execute(statement).await?;
    }
    Ok(())
}

/// Deletes the Generation's rows that an attempt at `attempted_at` did not
/// write.
pub(super) async fn delete_rows_not_written(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    generation_id: &ProviderGenerationId,
    attempted_at_unix_nanos: i64,
) -> Result<(), Error> {
    transaction
        .execute(
            generation_rows(workspace_id, generation_id)
                .and_where(
                    Expr::col(SearchEntities::IndexedAtUnixNanos).ne(attempted_at_unix_nanos),
                )
                .to_owned(),
        )
        .await?;
    Ok(())
}

/// Deletes every Operation row of the Generation, then refreshes the
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

/// Upserts the Generation's state row.
pub(super) async fn write_generation_state(
    transaction: &mut Transaction<'_>,
    state: GenerationState<'_>,
) -> Result<(), Error> {
    let (name, error) = match state.state {
        AttemptState::Complete => (COMPLETE, None),
        AttemptState::Failed { error } => (FAILED, Some(error)),
    };
    transaction
        .execute(
            Query::insert()
                .into_table(SearchIndexStates::Table)
                .columns([
                    SearchIndexStates::WorkspaceId,
                    SearchIndexStates::TargetId,
                    SearchIndexStates::Kind,
                    SearchIndexStates::ProviderId,
                    SearchIndexStates::ProviderGenerationId,
                    SearchIndexStates::ProviderDisplayName,
                    SearchIndexStates::State,
                    SearchIndexStates::AttemptedAtUnixNanos,
                    SearchIndexStates::Error,
                ])
                .values_panic([
                    state.workspace_id.to_string().into(),
                    state.generation_id.to_string().into(),
                    OPERATION.into(),
                    state.provider_id.to_string().into(),
                    state.generation_id.to_string().into(),
                    state.display_name.into(),
                    name.into(),
                    state.attempted_at_unix_nanos.into(),
                    error.into(),
                ])
                .on_conflict(
                    OnConflict::column(SearchIndexStates::TargetId)
                        .update_columns([
                            SearchIndexStates::ProviderDisplayName,
                            SearchIndexStates::State,
                            SearchIndexStates::AttemptedAtUnixNanos,
                            SearchIndexStates::Error,
                        ])
                        .to_owned(),
                )
                .to_owned(),
        )
        .await?;
    Ok(())
}

/// Deletes one Generation's Operation rows and state row, then refreshes the
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
                .and_where(Expr::col(SearchIndexStates::Kind).eq(OPERATION))
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
                .and_where(Expr::col(SearchEntities::Kind).eq(OPERATION))
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
                .and_where(Expr::col(SearchIndexStates::Kind).eq(OPERATION))
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

/// Deletes the Operation rows and state rows of every Generation of one
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
                .and_where(Expr::col(SearchEntities::Kind).eq(OPERATION))
                .and_where(Expr::col(SearchEntities::WorkspaceId).eq(workspace_id.as_str()))
                .and_where(Expr::col(SearchEntities::ProviderId).eq(provider_id.as_str()))
                .to_owned(),
        )
        .await?;
    transaction
        .execute(
            Query::delete()
                .from_table(SearchIndexStates::Table)
                .and_where(Expr::col(SearchIndexStates::Kind).eq(OPERATION))
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
/// function id; the fingerprint is the definition revision.
pub(super) struct FunctionRow<'a> {
    pub(super) workspace_id: &'a WorkspaceId,
    pub(super) function_id: &'a SavedFunctionId,
    pub(super) principal: &'a Principal,
    pub(super) call_path: CallPath,
    pub(super) qualified_name: &'a str,
    pub(super) description: &'a str,
    pub(super) searchable_text: String,
    pub(super) revision: u64,
    pub(super) indexed_at_unix_nanos: i64,
}

impl FunctionRow<'_> {
    fn values(&self) -> [SimpleExpr; FUNCTION_COLUMNS.len()] {
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
            self.revision.to_string().into(),
            self.indexed_at_unix_nanos.into(),
        ]
    }
}

/// One failed Saved Function attempt's row in `search_index_states`.
pub(super) struct FailedFunctionState<'a> {
    pub(super) workspace_id: &'a WorkspaceId,
    pub(super) function_id: &'a SavedFunctionId,
    pub(super) principal: &'a Principal,
    pub(super) attempted_at_unix_nanos: i64,
    pub(super) error: String,
}

/// Upserts a Saved Function row by Workspace and document id. An update,
/// never `INSERT OR REPLACE`, for the reason [`upsert_operation_rows`] gives.
pub(super) async fn upsert_function_row(
    transaction: &mut Transaction<'_>,
    row: &FunctionRow<'_>,
) -> Result<(), Error> {
    transaction
        .execute(
            Query::insert()
                .into_table(SearchEntities::Table)
                .columns(FUNCTION_COLUMNS)
                .values_panic(row.values())
                .on_conflict(
                    OnConflict::columns([SearchEntities::WorkspaceId, SearchEntities::DocId])
                        .update_columns(FUNCTION_COLUMNS.into_iter().skip(2))
                        .to_owned(),
                )
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
                .columns([
                    SearchIndexStates::WorkspaceId,
                    SearchIndexStates::TargetId,
                    SearchIndexStates::Kind,
                    SearchIndexStates::SavedFunctionId,
                    SearchIndexStates::PrincipalId,
                    SearchIndexStates::PrincipalKind,
                    SearchIndexStates::State,
                    SearchIndexStates::AttemptedAtUnixNanos,
                    SearchIndexStates::Error,
                ])
                .values_panic([
                    state.workspace_id.to_string().into(),
                    function_id.clone().into(),
                    SAVED_FUNCTION.into(),
                    function_id.into(),
                    state.principal.id().as_str().into(),
                    state.principal.kind().as_storage_str().into(),
                    FAILED.into(),
                    state.attempted_at_unix_nanos.into(),
                    state.error.into(),
                ])
                .on_conflict(
                    OnConflict::column(SearchIndexStates::TargetId)
                        .update_columns([
                            SearchIndexStates::AttemptedAtUnixNanos,
                            SearchIndexStates::Error,
                        ])
                        .to_owned(),
                )
                .to_owned(),
        )
        .await?;
    Ok(())
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
}

/// The outcome a Generation's state row records.
pub(super) enum RecordedOutcome {
    /// Indexed under this Provider display name.
    Complete { display_name: String },
    /// Failed at this time.
    Failed { attempted_at_unix_nanos: i64 },
}

/// Reads every Generation's state row in every Workspace.
pub(super) async fn recorded_generations(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<RecordedGeneration>, IndexError> {
    let rows: Vec<(String, String, String, String, i64)> = transaction
        .fetch_all(
            Query::select()
                .columns([
                    SearchIndexStates::WorkspaceId,
                    SearchIndexStates::ProviderGenerationId,
                    SearchIndexStates::State,
                    SearchIndexStates::ProviderDisplayName,
                    SearchIndexStates::AttemptedAtUnixNanos,
                ])
                .from(SearchIndexStates::Table)
                .and_where(Expr::col(SearchIndexStates::Kind).eq(OPERATION))
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(
            |(workspace_id, generation_id, state, display_name, attempted_at_unix_nanos)| {
                // The state check admits only `complete` and `failed`.
                let outcome = if state == COMPLETE {
                    RecordedOutcome::Complete { display_name }
                } else {
                    RecordedOutcome::Failed {
                        attempted_at_unix_nanos,
                    }
                };
                Ok(RecordedGeneration {
                    generation: workspace_generation(&workspace_id, &generation_id)?,
                    outcome,
                })
            },
        )
        .collect()
}

/// Reads a Workspace's Operation rows as `(provider id, Generation id, Call
/// Path, Provider name)`, in Call Path order, for tests in crates that start
/// Coral.
#[cfg(any(test, feature = "test-fixtures"))]
pub(crate) async fn operation_rows_in_workspace(
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
                .and_where(Expr::col(SearchEntities::Kind).eq(OPERATION))
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
                .and_where(Expr::col(SearchIndexStates::Kind).eq(OPERATION))
                .order_by(
                    SearchIndexStates::ProviderGenerationId,
                    sea_query::Order::Asc,
                )
                .to_owned(),
        )
        .await
}

/// Reads every Generation that has Operation rows, each once.
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
                .and_where(Expr::col(SearchEntities::Kind).eq(OPERATION))
                .to_owned(),
        )
        .await?;
    rows.iter()
        .map(|(workspace_id, generation_id)| workspace_generation(workspace_id, generation_id))
        .collect()
}

/// Deletes one Generation's Operation rows and state row within its
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
                .and_where(Expr::col(SearchIndexStates::Kind).eq(OPERATION))
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
    /// The definition revision the row indexed, as text.
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

/// Reads every Saved Function whose last attempt failed.
pub(super) async fn failed_functions(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<WorkspaceFunction>, IndexError> {
    let rows: Vec<(String, String)> = transaction
        .fetch_all(
            Query::select()
                .columns([
                    SearchIndexStates::WorkspaceId,
                    SearchIndexStates::SavedFunctionId,
                ])
                .from(SearchIndexStates::Table)
                .and_where(Expr::col(SearchIndexStates::Kind).eq(SAVED_FUNCTION))
                .to_owned(),
        )
        .await?;
    rows.iter()
        .map(|(workspace_id, function_id)| workspace_function(workspace_id, function_id))
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
    let rows: Vec<WatermarkColumns> = transaction
        .fetch_all_sql(
            "SELECT coalesce(e.workspace_id, s.workspace_id), \
                 s.counted_rows, s.counted_max_indexed_at, \
                 coalesce(e.row_count, 0), e.max_indexed_at \
             FROM ( \
                 SELECT workspace_id, count(*) AS row_count, \
                     max(indexed_at_unix_nanos) AS max_indexed_at \
                 FROM search_entities GROUP BY workspace_id \
             ) AS e \
             FULL JOIN search_stats AS s ON s.workspace_id = e.workspace_id",
            sea_query::Values(Vec::new()),
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

/// On Postgres, recomputes `doc_len` for one Generation's rows: the number of
/// lexeme positions in the row's vector. `SQLite` needs none.
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
             WHERE d.workspace_id = $1 AND d.provider_generation_id = $2",
            sea_query::Values(vec![
                workspace_id.to_string().into(),
                generation_id.to_string().into(),
            ]),
        )
        .await?;
    Ok(())
}

/// On Postgres, recomputes `doc_len` for one Saved Function's row, as
/// [`refresh_generation_lengths`] does for a Generation's.
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
             WHERE d.workspace_id = $1 AND d.saved_function_id = $2",
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
/// of the statement that calls it. Returns the rows written or deleted.
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
    // The sub-statements of one statement must not write one row twice, so
    // the delete takes only the terms the count no longer holds.
    let (written,): (i64,) = transaction
        .fetch_all_sql(
            "WITH counted AS MATERIALIZED ( \
                 SELECT word AS term, ndoc \
                 FROM ts_stat(format( \
                     'SELECT tsv FROM search_entities WHERE workspace_id = %L', $1::text \
                 )) \
             ), \
             gone AS ( \
                 DELETE FROM search_terms AS t \
                 WHERE t.workspace_id = $1::text \
                   AND NOT EXISTS (SELECT 1 FROM counted AS c WHERE c.term = t.term) \
                 RETURNING 1 \
             ), \
             terms AS ( \
                 INSERT INTO search_terms (workspace_id, term, ndoc) \
                 SELECT $1::text, term, ndoc FROM counted \
                 ON CONFLICT (workspace_id, term) DO UPDATE SET ndoc = excluded.ndoc \
                 RETURNING 1 \
             ), \
             totals AS ( \
                 INSERT INTO search_stats ( \
                     workspace_id, total_docs, avgdl, counted_rows, counted_max_indexed_at \
                 ) \
                 SELECT $1::text, count(*), coalesce(avg(doc_len), 1.0), \
                     count(*), max(indexed_at_unix_nanos) \
                 FROM search_entities WHERE workspace_id = $1::text \
                 ON CONFLICT (workspace_id) DO UPDATE \
                 SET total_docs = excluded.total_docs, avgdl = excluded.avgdl, \
                     counted_rows = excluded.counted_rows, \
                     counted_max_indexed_at = excluded.counted_max_indexed_at \
                 RETURNING 1 \
             ) \
             SELECT (SELECT count(*) FROM gone) + (SELECT count(*) FROM terms) \
                 + (SELECT count(*) FROM totals)",
            sea_query::Values(vec![workspace_id.into()]),
        )
        .await?
        .into_iter()
        .next()
        .ok_or(sqlx::Error::RowNotFound)?;
    // A sum of counts is never negative.
    Ok(written.unsigned_abs())
}
