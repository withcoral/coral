//! The Authored Tool indexer: one Private or Shared Tool's entry in, one row
//! out. Both kinds take one path, and a row's kind says which it is.
//!
//! A Private Tool's delete and a Shared Tool's removal never touch these
//! rows (ADR 0017). [`sync`] removes them once it finds the Tool gone, and a
//! reconcile pass finds the rows whose notification never came, such as
//! those of a removed Principal.
//!
//! [`sync`]: AuthoredToolIndexer::sync

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;

use async_trait::async_trait;
use coral_db::Transaction;
use coral_tools::{AuthoredToolEntry, AuthoredToolId, AuthoredToolIndexRevision};
use coral_workspace::WorkspaceId;

use super::repository::{
    self, AuthoredToolFailure, AuthoredToolRow, FailedAuthoredToolState, WorkspaceAuthoredTool,
};
use super::retry::RetryRule;
use super::{DistinctWords, EntityIndexer, IndexError, compact_identifier_variant, now_unix_nanos};
use crate::CallPath;

/// Indexes Private Tools and Shared Tools.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct AuthoredToolIndexer;

/// The search rows of one Authored Tool that no longer exists: a deleted
/// Private Tool, or a removed Shared Tool.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct AuthoredToolRemoval {
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) id: AuthoredToolId,
}

/// What it takes to bring the index in line with one Authored Tool.
pub(crate) enum AuthoredToolSync {
    /// The Tool exists and the index lacks it, holds another revision of
    /// it, or holds a failure of it that is due again.
    Index(Box<AuthoredToolEntry>),
    /// The Tool is gone and the index still holds rows of it.
    Remove(AuthoredToolRemoval),
    /// The index already agrees with the Tool.
    Nothing,
}

#[async_trait]
impl EntityIndexer for AuthoredToolIndexer {
    type Entry = AuthoredToolEntry;
    type Rows<'e> = AuthoredToolRow<'e>;
    type Removal = AuthoredToolRemoval;
    type Expected = AuthoredToolEntry;

    fn workspace_id<'e>(&self, entry: &'e AuthoredToolEntry) -> &'e WorkspaceId {
        entry.workspace_id()
    }

    fn rows<'e>(&self, entry: &'e AuthoredToolEntry) -> Result<AuthoredToolRow<'e>, IndexError> {
        authored_tool_row(entry)
    }

    async fn index(
        &self,
        transaction: &mut Transaction<'_>,
        entry: &AuthoredToolEntry,
        row: &AuthoredToolRow<'_>,
    ) -> Result<super::IndexEffect, IndexError> {
        repository::upsert_authored_tool_row(transaction, row, now_unix_nanos()?).await?;
        repository::delete_authored_tool_state(transaction, entry.workspace_id(), entry.id())
            .await?;
        Ok(super::IndexEffect::Updated)
    }

    /// Removes the Tool's row and state row. A failed Tool has a state row
    /// alone, whose removal leaves the statistics as they were.
    async fn remove(
        &self,
        transaction: &mut Transaction<'_>,
        removal: &AuthoredToolRemoval,
    ) -> Result<Option<WorkspaceId>, IndexError> {
        let removed =
            repository::delete_authored_tool_entry(transaction, &removal.workspace_id, &removal.id)
                .await?;
        Ok((removed > 0).then(|| removal.workspace_id.clone()))
    }

    /// Returns each current Authored Tool to index again:
    /// - one with a `failed` state row that another revision recorded, or
    ///   whose retry rule is due at `now_unix_nanos`;
    /// - one without such a row whose row is missing, or whose fingerprint is
    ///   not [`repository::authored_tool_fingerprint`] of its revision.
    ///
    /// A failure of the current revision that is not yet due waits for a
    /// later pass, whatever the Tool's row holds. [`Self::orphaned`] finds
    /// what a pass must remove.
    ///
    /// The fingerprint holds the Tool's revision and the indexer
    /// revision, and is compared for equality only. So, unlike a Generation,
    /// a row that a newer release wrote is indexed again. During a rolling
    /// update the two releases rewrite each other's rows until the older one
    /// stops.
    async fn reconcile<'e>(
        &self,
        transaction: &mut Transaction<'_>,
        expected: &'e [AuthoredToolEntry],
        now_unix_nanos: i64,
    ) -> Result<Vec<&'e AuthoredToolEntry>, IndexError> {
        let fingerprints = repository::recorded_authored_tools(transaction)
            .await?
            .into_iter()
            .map(|row| (row.tool, row.fingerprint))
            .collect::<HashMap<_, _>>();
        let failures = repository::failed_authored_tools(transaction)
            .await?
            .into_iter()
            .map(|failed| (failed.tool, failed.failure))
            .collect::<HashMap<_, _>>();
        Ok(expected
            .iter()
            .filter(|entry| {
                let key = WorkspaceAuthoredTool {
                    workspace_id: entry.workspace_id().clone(),
                    id: entry.id().clone(),
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

/// The rule of [`AuthoredToolIndexer::reconcile`] for one Authored Tool, its
/// failure, if any, and its row's fingerprint, if it has a row.
fn needs_indexing(
    failure: Option<&AuthoredToolFailure>,
    fingerprint: Option<&str>,
    entry: &AuthoredToolEntry,
    now_unix_nanos: i64,
) -> bool {
    match failure {
        Some(failure) => {
            failure.revision != Some(entry.revision()) || failure.retry.is_due(now_unix_nanos)
        }
        None => {
            fingerprint != Some(repository::authored_tool_fingerprint(entry.revision()).as_str())
        }
    }
}

impl AuthoredToolIndexer {
    /// What it takes, read now, to bring the index in line with the Tool:
    /// the one rule for a save, a delete, a publish, a removal and a
    /// reconcile pass, so each may run late, twice or out of order. A
    /// notification names an id alone, so a late one reads the Tool as it
    /// stands, never an older copy.
    pub(crate) async fn sync(
        &self,
        transaction: &mut Transaction<'_>,
        workspace_id: &WorkspaceId,
        id: &AuthoredToolId,
        now_unix_nanos: i64,
    ) -> Result<AuthoredToolSync, IndexError> {
        let entry =
            coral_tools::authored_tool_entry_in_transaction(transaction, workspace_id, id).await?;
        let failure = repository::failed_authored_tool(transaction, workspace_id, id).await?;
        let fingerprint =
            repository::recorded_authored_tool_fingerprint(transaction, workspace_id, id).await?;
        Ok(match entry {
            Some(entry)
                if needs_indexing(
                    failure.as_ref(),
                    fingerprint.as_deref(),
                    &entry,
                    now_unix_nanos,
                ) =>
            {
                AuthoredToolSync::Index(Box::new(entry))
            }
            None if failure.is_some() || fingerprint.is_some() => {
                AuthoredToolSync::Remove(AuthoredToolRemoval {
                    workspace_id: workspace_id.clone(),
                    id: id.clone(),
                })
            }
            Some(_) | None => AuthoredToolSync::Nothing,
        })
    }

    /// The removal of every Authored Tool the index holds and `current`
    /// does not: every Private Tool and every live Shared Tool that exists
    /// now. Reads only, so a read snapshot serves it and a pass that finds
    /// none never takes the writer.
    ///
    /// `transaction` must be the snapshot `current` was read from, so a
    /// Tool saved or published later is invisible to both reads. The pass
    /// then runs [`Self::sync`] for each one, which reads the Tool again.
    pub(crate) async fn orphaned(
        &self,
        transaction: &mut Transaction<'_>,
        current: &[AuthoredToolIndexRevision],
    ) -> Result<Vec<AuthoredToolRemoval>, IndexError> {
        let current = current
            .iter()
            .map(|tool| (tool.workspace_id(), tool.id()))
            .collect::<HashSet<_>>();
        let recorded = repository::recorded_authored_tools(transaction).await?;
        let failed = repository::failed_authored_tools(transaction).await?;
        Ok(recorded
            .into_iter()
            .map(|row| row.tool)
            .chain(failed.into_iter().map(|failed| failed.tool))
            .filter(|held| !current.contains(&(&held.workspace_id, &held.id)))
            .map(|held| AuthoredToolRemoval {
                workspace_id: held.workspace_id,
                id: held.id,
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect())
    }
}

/// Records a failed attempt for the Authored Tool: a `failed` state row with the
/// error, the revision it tried, and its retry rule under
/// `reconcile_interval`, and no row in the search table. The caller runs
/// this in a fresh transaction after the attempt's transaction rolled back.
/// A row an earlier attempt wrote goes too: the search table holds only what
/// indexed, so an Authored Tool whose last attempt failed does not rank on stale
/// text. Answers whether that row went.
pub(crate) async fn record_failed_attempt(
    transaction: &mut Transaction<'_>,
    entry: &AuthoredToolEntry,
    error: &IndexError,
    reconcile_interval: Duration,
) -> Result<bool, IndexError> {
    // A failure of another revision starts the count again.
    let prior = repository::failed_authored_tool(transaction, entry.workspace_id(), entry.id())
        .await?
        .filter(|failure| failure.revision == Some(entry.revision()))
        .map(|failure| failure.retry);
    let removed =
        repository::delete_authored_tool_row(transaction, entry.workspace_id(), entry.id()).await?;
    let attempted_at = now_unix_nanos()?;
    repository::write_failed_authored_tool_state(
        transaction,
        FailedAuthoredToolState {
            workspace_id: entry.workspace_id(),
            id: entry.id(),
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

fn authored_tool_row(entry: &AuthoredToolEntry) -> Result<AuthoredToolRow<'_>, IndexError> {
    let call_path = CallPath::authored_tool(entry.path())
        .map_err(|error| IndexError::InvalidEntry(error.to_string()))?;
    Ok(AuthoredToolRow {
        workspace_id: entry.workspace_id(),
        id: entry.id(),
        principal: entry.principal(),
        call_path,
        qualified_name: entry.path().relative_path(),
        description: entry.description().as_str(),
        searchable_text: searchable_text(entry),
        field_names: crate::index_text::field_names(
            entry
                .declaration_fields()
                .unwrap_or_default()
                .iter()
                .map(coral_tools::AuthoredToolDeclarationField::path),
        ),
        revision: entry.revision(),
    })
}

/// The lowest-weight text: the words of every declaration path with its
/// text and of the compact identifier variant of the path, each once, as
/// [`DistinctWords`] keeps them. An Authored Tool whose declaration fields did not
/// project is searchable by path and description. Raise
/// [`super::INDEXER_REVISION`] when this text changes.
fn searchable_text(entry: &AuthoredToolEntry) -> String {
    let variant = compact_identifier_variant(entry.path().relative_path());
    let mut words = DistinctWords::default();
    for field in entry.declaration_fields().unwrap_or_default() {
        words.push_field(field.path(), field.text());
    }
    if let Some(variant) = &variant {
        words.push(variant);
    }
    words.into_text()
}
