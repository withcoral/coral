//! The write side of Universal Search: one [`EntityIndexer`] per Search
//! Entity kind, the sibling of the retriever that reads the same rows.
//!
//! An indexer writes the rows of its kind into the search-owned tables and
//! reads nothing but those tables. It joins the caller's transaction;
//! [`crate::SearchIndex`] owns when each transaction begins and ends.

pub(crate) mod functions;
pub(crate) mod operations;
mod repository;

#[cfg(any(test, feature = "test-fixtures"))]
pub(crate) use repository::{generation_states_in_workspace, operation_rows_in_workspace};

use async_trait::async_trait;
use coral_db::Transaction;
use coral_workspace::WorkspaceId;

/// A failure to write the search index. The caller's transaction rolls back.
#[derive(Debug, thiserror::Error)]
pub(crate) enum IndexError {
    /// An entry cannot become a row, for example an invalid Call Path.
    #[error("invalid search entry: {0}")]
    InvalidEntry(String),
    /// The write did not finish within its limit and was rolled back.
    #[error("the index write did not finish within {} seconds", .0.as_secs())]
    TimedOut(std::time::Duration),
    /// The system clock cannot stamp the attempt.
    #[error("system clock is before 1970 or past the durable nanosecond range")]
    Clock,
    /// A search-owned row holds a value its column does not admit.
    #[error("corrupt search index: {0}")]
    Corrupt(&'static str),
    /// Reading the Providers' active Generations or preparing one failed.
    #[error(transparent)]
    Provider(#[from] coral_provider::Error),
    /// Reading the current Saved Functions failed.
    #[error(transparent)]
    SavedFunctions(#[from] coral_functions::Error),
    /// Database access failed.
    #[error(transparent)]
    Database(#[from] coral_db::Error),
}

/// Writes one Search Entity kind into the search-owned tables.
#[async_trait]
pub(crate) trait EntityIndexer: Send + Sync {
    /// What a notification hands over, prepared by the owner.
    type Entry: Send + Sync;
    /// What a removal names; `Infallible` when a database cascade removes.
    type Removal: Send + Sync;
    /// What the reconcile task compares the index against.
    type Expected: Send + Sync;

    /// Writes the entry's rows and state, all or nothing within the
    /// transaction. Writing the same entry again leaves the same rows.
    async fn index(
        &self,
        transaction: &mut Transaction<'_>,
        entry: &Self::Entry,
    ) -> Result<(), IndexError>;

    /// Removes the rows and state the removal names.
    async fn remove(
        &self,
        transaction: &mut Transaction<'_>,
        removal: &Self::Removal,
    ) -> Result<(), IndexError>;

    /// Compares the index with `expected`, everything the owners hold now,
    /// and returns what `expected` holds and the index lacks, holds stale, or
    /// holds as failed; a failure attempted at or after
    /// `retry_failed_from_unix_nanos` may be left for a later pass. Reads
    /// only, so a read snapshot serves it.
    ///
    /// The caller indexes each returned item in a transaction of its own, so
    /// one failure stays with its item. Once those writes commit, the same
    /// comparison returns nothing.
    async fn reconcile<'e>(
        &self,
        transaction: &mut Transaction<'_>,
        expected: &'e [Self::Expected],
        retry_failed_from_unix_nanos: i64,
    ) -> Result<Vec<&'e Self::Expected>, IndexError>;
}

/// Reads every Workspace whose Postgres statistics a reconcile pass
/// rewrites: each that has rows or statistics. `SQLite` has none.
pub(crate) async fn workspaces_with_statistics(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<WorkspaceId>, IndexError> {
    repository::workspaces_with_statistics(transaction).await
}

/// On Postgres, rewrites one Workspace's ranking statistics from its rows.
/// Returns the statistics rows written or deleted.
pub(crate) async fn refresh_statistics(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<u64, IndexError> {
    Ok(repository::refresh_workspace_statistics(transaction, workspace_id).await?)
}

fn now_unix_nanos() -> Result<i64, IndexError> {
    crate::unix_now_nanos().ok_or(IndexError::Clock)
}

/// The compact identifier variant of a name: lowercase, every character that
/// is not alphanumeric removed. `deploy_url` gives `deployurl`. `None` when
/// the variant adds nothing. Indexed beside the name, it gives a compact
/// query variant a whole lexeme to meet. Retrievers derive the query's
/// variants with the same rule.
pub(crate) fn compact_identifier_variant(value: &str) -> Option<String> {
    let normalized = value.trim().to_lowercase();
    let compact = normalized
        .chars()
        .filter(|character| character.is_alphanumeric())
        .collect::<String>();
    (!compact.is_empty() && compact != normalized).then_some(compact)
}
