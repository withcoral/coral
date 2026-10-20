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

use super::repository::{self, FailedFunctionState, FunctionRow, WorkspaceFunction};
use super::retry::RetryRule;
use super::{EntityIndexer, IndexError, compact_identifier_variant, now_unix_nanos};
use crate::CallPath;

/// Indexes Saved Functions.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct SavedFunctionIndexer;

#[async_trait]
impl EntityIndexer for SavedFunctionIndexer {
    type Entry = SavedFunctionEntry;
    type Removal = Infallible;
    type Expected = SavedFunctionEntry;

    fn workspace_id<'e>(&self, entry: &'e SavedFunctionEntry) -> &'e WorkspaceId {
        entry.workspace_id()
    }

    async fn index(
        &self,
        transaction: &mut Transaction<'_>,
        entry: &SavedFunctionEntry,
    ) -> Result<(), IndexError> {
        let row = function_row(entry, now_unix_nanos()?)?;
        repository::upsert_function_row(transaction, &row).await?;
        repository::delete_function_state(transaction, entry.workspace_id(), entry.id()).await?;
        repository::refresh_function_length(transaction, entry.workspace_id(), entry.id()).await?;
        repository::refresh_workspace_statistics(transaction, entry.workspace_id()).await?;
        Ok(())
    }

    async fn remove(
        &self,
        _transaction: &mut Transaction<'_>,
        removal: &Infallible,
    ) -> Result<(), IndexError> {
        match *removal {}
    }

    /// Returns each current function with a `failed` state row whose retry
    /// rule is due at `now_unix_nanos` or that another revision recorded,
    /// and each function without such a row that has no row or a row whose
    /// fingerprint differs from its definition revision. A failure of the
    /// current revision that is not yet due waits for a later pass whatever
    /// the function's row holds. Nothing is left to remove: a function's row
    /// and state row cascade with it.
    ///
    /// The fingerprint is an opaque version marker, compared for equality
    /// only. As text, revision 10 would sort before revision 9.
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
            .map(|failed| (failed.function, (failed.retry, failed.revision)))
            .collect::<HashMap<_, _>>();
        Ok(expected
            .iter()
            .filter(|entry| {
                let key = WorkspaceFunction {
                    workspace_id: entry.workspace_id().clone(),
                    function_id: entry.id().clone(),
                };
                match failures.get(&key) {
                    Some((retry, revision)) => {
                        *revision != Some(entry.revision()) || retry.is_due(now_unix_nanos)
                    }
                    None => fingerprints.get(&key) != Some(&entry.revision().to_string()),
                }
            })
            .collect())
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
    let prior = repository::failed_function_retry_rule(
        transaction,
        entry.workspace_id(),
        entry.id(),
        entry.revision(),
    )
    .await?;
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

fn function_row(
    entry: &SavedFunctionEntry,
    indexed_at_unix_nanos: i64,
) -> Result<FunctionRow<'_>, IndexError> {
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
        indexed_at_unix_nanos,
    })
}

/// The lowest-weight text: every declaration path with its text, one per
/// line, and the compact identifier variant of the path. A function whose
/// declaration fields did not project is searchable by path and description.
fn searchable_text(entry: &SavedFunctionEntry) -> String {
    // As for Operations, a field's text usually opens with its path; the path
    // is written once.
    let declarations = entry
        .declaration_fields()
        .unwrap_or_default()
        .iter()
        .flat_map(|field| {
            let path = field.path();
            std::iter::once(path).chain(field.text().lines().filter(move |line| *line != path))
        });
    let variant = compact_identifier_variant(entry.path().as_str());
    declarations
        .chain(variant.as_deref())
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}
