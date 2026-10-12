//! Test-only faults in Universal Search's storage, and reads of what the
//! index holds, for crates that start Coral.
//!
//! The `test-fixtures` feature enables this module for dev-dependencies;
//! production builds never do.

use coral_db::Database;
use coral_workspace::WorkspaceId;

use crate::StorageError;

/// Records a search stream version newer than this binary applies, as a newer
/// Coral release would leave behind.
///
/// # Errors
///
/// Returns database failures.
pub async fn record_unsupported_stream_version(database: &Database) -> Result<(), StorageError> {
    crate::storage::record_unsupported_version(database).await
}

/// One Operation row of the index, as a test observes it: membership, never
/// counts or scores.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedOperation {
    /// The Provider that owns the Operation.
    pub provider_id: String,
    /// The Generation the row belongs to.
    pub generation_id: String,
    /// The Operation's Call Path.
    pub call_path: String,
    /// The Provider namespace and display name the row holds.
    pub provider_name: String,
}

/// The state row of one Generation's indexing attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedGeneration {
    /// The Provider that owns the Generation.
    pub provider_id: String,
    /// The Generation.
    pub generation_id: String,
    /// `complete` or `failed`.
    pub state: String,
    /// The Provider display name a `complete` attempt indexed.
    pub display_name: Option<String>,
}

/// Reads a Workspace's indexed Operations, in Call Path order.
///
/// # Errors
///
/// Returns database failures.
pub async fn indexed_operations(
    database: &Database,
    workspace_id: &WorkspaceId,
) -> Result<Vec<IndexedOperation>, coral_db::Error> {
    let mut transaction = database.begin_read_snapshot().await?;
    let rows = crate::indexer::operation_rows_in_workspace(&mut transaction, workspace_id).await?;
    transaction.rollback().await?;
    Ok(rows
        .into_iter()
        .map(
            |(provider_id, generation_id, call_path, provider_name)| IndexedOperation {
                provider_id,
                generation_id,
                call_path,
                provider_name,
            },
        )
        .collect())
}

/// Reads a Workspace's Generation state rows, in Generation id order.
///
/// # Errors
///
/// Returns database failures.
pub async fn indexed_generations(
    database: &Database,
    workspace_id: &WorkspaceId,
) -> Result<Vec<IndexedGeneration>, coral_db::Error> {
    let mut transaction = database.begin_read_snapshot().await?;
    let rows =
        crate::indexer::generation_states_in_workspace(&mut transaction, workspace_id).await?;
    transaction.rollback().await?;
    Ok(rows
        .into_iter()
        .map(
            |(provider_id, generation_id, state, display_name)| IndexedGeneration {
                provider_id,
                generation_id,
                state,
                display_name,
            },
        )
        .collect())
}
