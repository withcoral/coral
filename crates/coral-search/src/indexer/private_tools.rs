//! The Private Tool indexer: one private tool's entry in, one row out.
//!
//! A Private Tool's delete never touches these rows (ADR 0017). [`sync`] removes
//! them once it finds the Private Tool gone, and a reconcile pass finds the rows
//! whose notification never came, such as those of a removed Principal.
//!
//! [`sync`]: PrivateToolIndexer::sync

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;

use async_trait::async_trait;
use coral_db::Transaction;
use coral_tools::{PrivateToolEntry, PrivateToolId, PrivateToolIndexRevision};
use coral_workspace::WorkspaceId;

use super::repository::{
    self, FailedPrivateToolState, PrivateToolFailure, PrivateToolRow, WorkspacePrivateTool,
};
use super::retry::RetryRule;
use super::{DistinctWords, EntityIndexer, IndexError, compact_identifier_variant, now_unix_nanos};
use crate::CallPath;

/// Indexes Private Tools.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct PrivateToolIndexer;

/// The search rows of one Private Tool that no longer exists.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct PrivateToolRemoval {
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) private_tool_id: PrivateToolId,
}

/// What it takes to bring the index in line with one Private Tool.
pub(crate) enum PrivateToolSync {
    /// The Private Tool exists and the index lacks it, holds another revision of
    /// it, or holds a failure of it that is due again.
    Index(Box<PrivateToolEntry>),
    /// The Private Tool is gone and the index still holds rows of it.
    Remove(PrivateToolRemoval),
    /// The index already agrees with the Private Tool.
    Nothing,
}

#[async_trait]
impl EntityIndexer for PrivateToolIndexer {
    type Entry = PrivateToolEntry;
    type Rows<'e> = PrivateToolRow<'e>;
    type Removal = PrivateToolRemoval;
    type Expected = PrivateToolEntry;

    fn workspace_id<'e>(&self, entry: &'e PrivateToolEntry) -> &'e WorkspaceId {
        entry.workspace_id()
    }

    fn rows<'e>(&self, entry: &'e PrivateToolEntry) -> Result<PrivateToolRow<'e>, IndexError> {
        private_tool_row(entry)
    }

    async fn index(
        &self,
        transaction: &mut Transaction<'_>,
        entry: &PrivateToolEntry,
        row: &PrivateToolRow<'_>,
    ) -> Result<super::IndexEffect, IndexError> {
        repository::upsert_private_tool_row(transaction, row, now_unix_nanos()?).await?;
        repository::delete_private_tool_state(transaction, entry.workspace_id(), entry.id())
            .await?;
        Ok(super::IndexEffect::Updated)
    }

    /// Removes the Private Tool's row and state row. A failed Private Tool has a
    /// state row alone, whose removal leaves the statistics as they were.
    async fn remove(
        &self,
        transaction: &mut Transaction<'_>,
        removal: &PrivateToolRemoval,
    ) -> Result<Option<WorkspaceId>, IndexError> {
        let removed = repository::delete_private_tool_entry(
            transaction,
            &removal.workspace_id,
            &removal.private_tool_id,
        )
        .await?;
        Ok((removed > 0).then(|| removal.workspace_id.clone()))
    }

    /// Returns each current Private Tool to index again:
    /// - one with a `failed` state row that another definition revision
    ///   recorded, or whose retry rule is due at `now_unix_nanos`;
    /// - one without such a row whose row is missing, or whose fingerprint is
    ///   not [`repository::private_tool_fingerprint`] of its definition revision.
    ///
    /// A failure of the current definition revision that is not yet due
    /// waits for a later pass, whatever the Private Tool's row holds.
    /// [`Self::orphaned`] finds what a pass must remove.
    ///
    /// The fingerprint holds the definition revision and the indexer
    /// revision, and is compared for equality only. So, unlike a Generation,
    /// a row that a newer release wrote is indexed again. During a rolling
    /// update the two releases rewrite each other's rows until the older one
    /// stops.
    async fn reconcile<'e>(
        &self,
        transaction: &mut Transaction<'_>,
        expected: &'e [PrivateToolEntry],
        now_unix_nanos: i64,
    ) -> Result<Vec<&'e PrivateToolEntry>, IndexError> {
        let fingerprints = repository::recorded_private_tools(transaction)
            .await?
            .into_iter()
            .map(|row| (row.private_tool, row.fingerprint))
            .collect::<HashMap<_, _>>();
        let failures = repository::failed_private_tools(transaction)
            .await?
            .into_iter()
            .map(|failed| (failed.private_tool, failed.failure))
            .collect::<HashMap<_, _>>();
        Ok(expected
            .iter()
            .filter(|entry| {
                let key = WorkspacePrivateTool {
                    workspace_id: entry.workspace_id().clone(),
                    private_tool_id: entry.id().clone(),
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

/// The rule of [`PrivateToolIndexer::reconcile`] for one Private Tool, its
/// failure, if any, and its row's fingerprint, if it has a row.
fn needs_indexing(
    failure: Option<&PrivateToolFailure>,
    fingerprint: Option<&str>,
    entry: &PrivateToolEntry,
    now_unix_nanos: i64,
) -> bool {
    match failure {
        Some(failure) => {
            failure.revision != Some(entry.revision()) || failure.retry.is_due(now_unix_nanos)
        }
        None => {
            fingerprint != Some(repository::private_tool_fingerprint(entry.revision()).as_str())
        }
    }
}

impl PrivateToolIndexer {
    /// What it takes, read now, to bring the index in line with the Private Tool:
    /// the one rule for a save, a delete and a reconcile pass, so each may
    /// run late, twice or out of order. A notification names an id alone, so
    /// a late one reads the Private Tool as it stands, never an older copy.
    pub(crate) async fn sync(
        &self,
        transaction: &mut Transaction<'_>,
        workspace_id: &WorkspaceId,
        private_tool_id: &PrivateToolId,
        now_unix_nanos: i64,
    ) -> Result<PrivateToolSync, IndexError> {
        let entry = coral_tools::private_tool_entry_in_transaction(
            transaction,
            workspace_id,
            private_tool_id,
        )
        .await?;
        let failure =
            repository::failed_private_tool(transaction, workspace_id, private_tool_id).await?;
        let fingerprint = repository::recorded_private_tool_fingerprint(
            transaction,
            workspace_id,
            private_tool_id,
        )
        .await?;
        Ok(match entry {
            Some(entry)
                if needs_indexing(
                    failure.as_ref(),
                    fingerprint.as_deref(),
                    &entry,
                    now_unix_nanos,
                ) =>
            {
                PrivateToolSync::Index(Box::new(entry))
            }
            None if failure.is_some() || fingerprint.is_some() => {
                PrivateToolSync::Remove(PrivateToolRemoval {
                    workspace_id: workspace_id.clone(),
                    private_tool_id: private_tool_id.clone(),
                })
            }
            Some(_) | None => PrivateToolSync::Nothing,
        })
    }

    /// The removal of every Private Tool the index holds and `current` does not:
    /// every Private Tool that exists now. Reads only, so a read snapshot serves
    /// it and a pass that finds none never takes the writer.
    ///
    /// `transaction` must be the snapshot `current` was read from, so a
    /// Private Tool saved later is invisible to both reads. The pass then runs
    /// [`Self::sync`] for each one, which reads the Private Tool again.
    pub(crate) async fn orphaned(
        &self,
        transaction: &mut Transaction<'_>,
        current: &[PrivateToolIndexRevision],
    ) -> Result<Vec<PrivateToolRemoval>, IndexError> {
        let current = current
            .iter()
            .map(|private_tool| (private_tool.workspace_id(), private_tool.id()))
            .collect::<HashSet<_>>();
        let recorded = repository::recorded_private_tools(transaction).await?;
        let failed = repository::failed_private_tools(transaction).await?;
        Ok(recorded
            .into_iter()
            .map(|row| row.private_tool)
            .chain(failed.into_iter().map(|failed| failed.private_tool))
            .filter(|held| !current.contains(&(&held.workspace_id, &held.private_tool_id)))
            .map(|held| PrivateToolRemoval {
                workspace_id: held.workspace_id,
                private_tool_id: held.private_tool_id,
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect())
    }
}

/// Records a failed attempt for the Private Tool: a `failed` state row with the
/// error, the revision it tried, and its retry rule under
/// `reconcile_interval`, and no row in the search table. The caller runs
/// this in a fresh transaction after the attempt's transaction rolled back.
/// A row an earlier attempt wrote goes too: the search table holds only what
/// indexed, so a Private Tool whose last attempt failed does not rank on stale
/// text. Answers whether that row went.
pub(crate) async fn record_failed_attempt(
    transaction: &mut Transaction<'_>,
    entry: &PrivateToolEntry,
    error: &IndexError,
    reconcile_interval: Duration,
) -> Result<bool, IndexError> {
    // A failure of another revision starts the count again.
    let prior = repository::failed_private_tool(transaction, entry.workspace_id(), entry.id())
        .await?
        .filter(|failure| failure.revision == Some(entry.revision()))
        .map(|failure| failure.retry);
    let removed =
        repository::delete_private_tool_row(transaction, entry.workspace_id(), entry.id()).await?;
    let attempted_at = now_unix_nanos()?;
    repository::write_failed_private_tool_state(
        transaction,
        FailedPrivateToolState {
            workspace_id: entry.workspace_id(),
            private_tool_id: entry.id(),
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
    Ok(removed > 0)
}

fn private_tool_row(entry: &PrivateToolEntry) -> Result<PrivateToolRow<'_>, IndexError> {
    let qualified_name = entry.path().as_str();
    let call_path = CallPath::private_tool(qualified_name)
        .map_err(|error| IndexError::InvalidEntry(error.to_string()))?;
    Ok(PrivateToolRow {
        workspace_id: entry.workspace_id(),
        private_tool_id: entry.id(),
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
/// [`DistinctWords`] keeps them. A Private Tool whose declaration fields did not
/// project is searchable by path and description. Raise
/// [`super::INDEXER_REVISION`] when this text changes.
fn searchable_text(entry: &PrivateToolEntry) -> String {
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
