//! The Saved Function indexer: one saved function's entry in, one row out.
//!
//! Removal needs no write: the row and any state row cascade from the
//! function, its Workspace, and, through the function, its Principal.

use std::collections::HashMap;
use std::convert::Infallible;
use std::time::Duration;

use async_trait::async_trait;
use coral_db::Transaction;
use coral_functions::SavedFunctionEntry;
use coral_workspace::WorkspaceId;

use super::repository::{
    self, FailedFunctionState, FunctionFailure, FunctionRow, WorkspaceFunction,
};
use super::retry::RetryRule;
use super::{DistinctWords, EntityIndexer, IndexError, compact_identifier_variant, now_unix_nanos};
use crate::CallPath;

/// Indexes Saved Functions.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct SavedFunctionIndexer;

#[async_trait]
impl EntityIndexer for SavedFunctionIndexer {
    type Entry = SavedFunctionEntry;
    type Rows<'e> = FunctionRow<'e>;
    type Removal = Infallible;
    type Expected = SavedFunctionEntry;

    fn workspace_id<'e>(&self, entry: &'e SavedFunctionEntry) -> &'e WorkspaceId {
        entry.workspace_id()
    }

    fn rows<'e>(&self, entry: &'e SavedFunctionEntry) -> Result<FunctionRow<'e>, IndexError> {
        function_row(entry)
    }

    async fn index(
        &self,
        transaction: &mut Transaction<'_>,
        entry: &SavedFunctionEntry,
        row: &FunctionRow<'_>,
    ) -> Result<super::IndexEffect, IndexError> {
        repository::upsert_function_row(transaction, row, now_unix_nanos()?).await?;
        repository::delete_function_state(transaction, entry.workspace_id(), entry.id()).await?;
        repository::refresh_function_length(transaction, entry.workspace_id(), entry.id()).await?;
        repository::refresh_workspace_statistics(transaction, entry.workspace_id()).await?;
        Ok(super::IndexEffect::Updated)
    }

    async fn remove(
        &self,
        _transaction: &mut Transaction<'_>,
        removal: &Infallible,
    ) -> Result<(), IndexError> {
        match *removal {}
    }

    /// Returns each current function to index again:
    /// - one with a `failed` state row that another definition revision
    ///   recorded, or whose retry rule is due at `now_unix_nanos`;
    /// - one without such a row whose row is missing, or whose fingerprint is
    ///   not [`repository::function_fingerprint`] of its definition revision.
    ///
    /// A failure of the current definition revision that is not yet due
    /// waits for a later pass, whatever the function's row holds. Nothing is
    /// left to remove: a function's row and state row cascade with it.
    ///
    /// The fingerprint holds the definition revision and the indexer
    /// revision, and is compared for equality only. So, unlike a Generation,
    /// a row that a newer release wrote is indexed again. During a rolling
    /// update the two releases rewrite each other's rows until the older one
    /// stops.
    async fn reconcile<'e>(
        &self,
        transaction: &mut Transaction<'_>,
        expected: &'e [SavedFunctionEntry],
        now_unix_nanos: i64,
    ) -> Result<Vec<&'e SavedFunctionEntry>, IndexError> {
        let fingerprints = repository::recorded_functions(transaction)
            .await?
            .into_iter()
            .map(|row| (row.function, row.fingerprint))
            .collect::<HashMap<_, _>>();
        let failures = repository::failed_functions(transaction)
            .await?
            .into_iter()
            .map(|failed| (failed.function, failed.failure))
            .collect::<HashMap<_, _>>();
        Ok(expected
            .iter()
            .filter(|entry| {
                let key = WorkspaceFunction {
                    workspace_id: entry.workspace_id().clone(),
                    function_id: entry.id().clone(),
                };
                needs_indexing(
                    failures.get(&key),
                    fingerprints.get(&key).map(String::as_str),
                    entry,
                    now_unix_nanos,
                )
            })
            .collect())
    }
}

/// The rule of [`SavedFunctionIndexer::reconcile`] for one function, its
/// failure, if any, and its row's fingerprint, if it has a row.
fn needs_indexing(
    failure: Option<&FunctionFailure>,
    fingerprint: Option<&str>,
    entry: &SavedFunctionEntry,
    now_unix_nanos: i64,
) -> bool {
    match failure {
        Some(failure) => {
            failure.revision != Some(entry.revision()) || failure.retry.is_due(now_unix_nanos)
        }
        None => fingerprint != Some(repository::function_fingerprint(entry.revision()).as_str()),
    }
}

impl SavedFunctionIndexer {
    /// Whether a pass that read `entry` as stale must still write it at
    /// `now_unix_nanos`. A function deleted or saved again since answers
    /// `false`: `entry` is an older copy, and writing it could replace what a
    /// notified write indexed. A later pass indexes the newer revision if its
    /// notification was lost. The rule of [`Self::reconcile`], for one
    /// function.
    pub(crate) async fn still_needs_indexing(
        &self,
        transaction: &mut Transaction<'_>,
        entry: &SavedFunctionEntry,
        now_unix_nanos: i64,
    ) -> Result<bool, IndexError> {
        let unchanged = coral_functions::saved_function_entries_in_transaction(
            transaction,
            entry.workspace_id(),
        )
        .await?
        .iter()
        .any(|current| current.id() == entry.id() && current.revision() == entry.revision());
        if !unchanged {
            return Ok(false);
        }
        let failure =
            repository::failed_function(transaction, entry.workspace_id(), entry.id()).await?;
        let fingerprint = repository::recorded_function_fingerprint(
            transaction,
            entry.workspace_id(),
            entry.id(),
        )
        .await?;
        Ok(needs_indexing(
            failure.as_ref(),
            fingerprint.as_deref(),
            entry,
            now_unix_nanos,
        ))
    }
}

/// Records a failed attempt for the function: a `failed` state row with the
/// error, the revision it tried, and its retry rule under
/// `reconcile_interval`, and no row in the search table. The caller runs
/// this in a fresh transaction after the attempt's transaction rolled back.
/// A row an earlier attempt wrote goes too: the search table holds only what
/// indexed, so a function whose last attempt failed does not rank on stale
/// text.
pub(crate) async fn record_failed_attempt(
    transaction: &mut Transaction<'_>,
    entry: &SavedFunctionEntry,
    error: &IndexError,
    reconcile_interval: Duration,
) -> Result<(), IndexError> {
    repository::lock_workspace(transaction, entry.workspace_id()).await?;
    // A failure of another revision starts the count again.
    let prior = repository::failed_function(transaction, entry.workspace_id(), entry.id())
        .await?
        .filter(|failure| failure.revision == Some(entry.revision()))
        .map(|failure| failure.retry);
    repository::delete_function_row(transaction, entry.workspace_id(), entry.id()).await?;
    let attempted_at = now_unix_nanos()?;
    repository::write_failed_function_state(
        transaction,
        FailedFunctionState {
            workspace_id: entry.workspace_id(),
            function_id: entry.id(),
            principal: entry.principal(),
            attempted_at_unix_nanos: attempted_at,
            error: error.to_string(),
            retry: RetryRule::after_failure(
                prior.as_ref(),
                error,
                attempted_at,
                reconcile_interval,
            ),
            revision: entry.revision(),
        },
    )
    .await?;
    Ok(())
}

fn function_row(entry: &SavedFunctionEntry) -> Result<FunctionRow<'_>, IndexError> {
    let qualified_name = entry.path().as_str();
    let call_path = CallPath::saved_function(qualified_name)
        .map_err(|error| IndexError::InvalidEntry(error.to_string()))?;
    Ok(FunctionRow {
        workspace_id: entry.workspace_id(),
        function_id: entry.id(),
        principal: entry.principal(),
        call_path,
        qualified_name,
        description: entry.description().as_str(),
        searchable_text: searchable_text(entry),
        revision: entry.revision(),
    })
}

/// The lowest-weight text: the words of every declaration path with its
/// text and of the compact identifier variant of the path, each once, as
/// [`DistinctWords`] keeps them. A function whose declaration fields did not
/// project is searchable by path and description. Raise
/// [`super::INDEXER_REVISION`] when this text changes.
fn searchable_text(entry: &SavedFunctionEntry) -> String {
    let variant = compact_identifier_variant(entry.path().as_str());
    let mut words = DistinctWords::default();
    for field in entry.declaration_fields().unwrap_or_default() {
        words.push_field(field.path(), field.text());
    }
    if let Some(variant) = &variant {
        words.push(variant);
    }
    words.into_text()
}
