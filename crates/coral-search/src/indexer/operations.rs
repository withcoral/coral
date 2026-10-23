//! The Operation indexer: one Provider Generation's entries in, one row per
//! Operation and the Generation's state row out.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;

use async_trait::async_trait;
use coral_catalog::{CatalogDiagnostic, OperationKey, ProviderGenerationId};
use coral_db::Transaction;
use coral_provider::{ActiveGeneration, GenerationEntries, OperationEntry, ProviderId};
use coral_workspace::WorkspaceId;

use super::repository::{self, AttemptState, GenerationState, OperationRow, RecordedOutcome};
use super::retry::RetryRule;
use super::{
    DistinctWords, EntityIndexer, INDEXER_REVISION, IndexError, compact_identifier_variant,
    now_unix_nanos,
};
use crate::CallPath;

/// Indexes the Operations of Provider Generations.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct OperationIndexer;

/// What an Operation removal names.
#[derive(Debug, Clone)]
pub(crate) enum OperationRemoval {
    /// Every Generation of a deleted Provider.
    Provider {
        workspace_id: WorkspaceId,
        provider_id: ProviderId,
    },
    /// One Generation that stopped being active.
    Generation(ProviderGenerationId),
}

#[async_trait]
impl EntityIndexer for OperationIndexer {
    type Entry = GenerationEntries;
    type Rows<'e> = GenerationRows<'e>;
    type Removal = OperationRemoval;
    type Expected = ActiveGeneration;

    fn workspace_id<'e>(&self, entries: &'e GenerationEntries) -> &'e WorkspaceId {
        entries.workspace_id()
    }

    /// Turns each entry into its row as the entries prepare it, so one
    /// Operation's declaration fields are held at a time.
    fn rows<'e>(&self, entries: &'e GenerationEntries) -> Result<GenerationRows<'e>, IndexError> {
        let mut rows = GenerationRows::default();
        for operation in entries.operations() {
            let operation = operation?;
            rows.warnings.extend(
                operation
                    .search_warnings()
                    .iter()
                    .map(|warning| (operation.key(), warning.clone())),
            );
            rows.rows.push(operation_row(entries, &operation)?);
        }
        Ok(rows)
    }

    async fn index(
        &self,
        transaction: &mut Transaction<'_>,
        entries: &GenerationEntries,
        rows: &GenerationRows<'_>,
    ) -> Result<(), IndexError> {
        let rows = &rows.rows;
        let workspace_id = entries.workspace_id();
        let generation_id = entries.generation_id();
        let attempted_at = now_unix_nanos()?;
        repository::upsert_operation_rows(transaction, rows, attempted_at).await?;
        // A Generation keeps its Operations, but the reconcile task prepares
        // entries under the current rules, which may leave one out; a rewrite
        // then removes that row.
        repository::delete_rows_not_written(transaction, workspace_id, generation_id, rows).await?;
        repository::write_generation_state(
            transaction,
            generation_state(entries.into(), attempted_at, AttemptState::Complete),
        )
        .await?;
        repository::refresh_generation_lengths(transaction, workspace_id, generation_id).await?;
        repository::refresh_workspace_statistics(transaction, workspace_id).await?;
        Ok(())
    }

    async fn remove(
        &self,
        transaction: &mut Transaction<'_>,
        removal: &OperationRemoval,
    ) -> Result<(), IndexError> {
        match removal {
            OperationRemoval::Provider {
                workspace_id,
                provider_id,
            } => {
                repository::lock_workspace(transaction, workspace_id).await?;
                repository::delete_provider(transaction, workspace_id, provider_id).await?;
            }
            OperationRemoval::Generation(generation_id) => {
                repository::delete_generation(transaction, generation_id).await?;
            }
        }
        Ok(())
    }

    /// Returns each active Generation with no state row, one whose state row
    /// an older indexer revision wrote, a `complete` one under another
    /// Provider display name, or a `failed` one whose retry rule is due at
    /// `now_unix_nanos`.
    ///
    /// A state row of a newer revision is left as it is: during a rolling
    /// update, two releases would otherwise index each other's Generations
    /// again on every pass.
    async fn reconcile<'e>(
        &self,
        transaction: &mut Transaction<'_>,
        expected: &'e [ActiveGeneration],
        now_unix_nanos: i64,
    ) -> Result<Vec<&'e ActiveGeneration>, IndexError> {
        let states = repository::recorded_generations(transaction)
            .await?
            .into_iter()
            .map(|state| (state.generation.generation_id.clone(), state))
            .collect::<HashMap<_, _>>();
        Ok(expected
            .iter()
            .filter(|generation| {
                let Some(state) = states.get(generation.generation_id()) else {
                    return true;
                };
                // `None` sorts before every revision.
                if state.indexer_revision < Some(INDEXER_REVISION) {
                    return true;
                }
                match &state.outcome {
                    // A rename keeps the Generation, so the name is compared on
                    // its own.
                    RecordedOutcome::Complete { display_name } => {
                        display_name != generation.display_name()
                    }
                    RecordedOutcome::Failed { retry } => retry.is_due(now_unix_nanos),
                }
            })
            .collect())
    }
}

impl OperationIndexer {
    /// Removes the rows and state rows of every Generation that is not in
    /// `active`, every Provider's active Generation: superseded ones, and
    /// those of a Provider deleted without a notification.
    ///
    /// Best effort across processes: a Generation another process activates
    /// and indexes after `active` was read counts as superseded here. Its
    /// rows and state row go, and the next pass indexes it again.
    ///
    /// Takes no Workspace lock and recounts no statistics: a delete lowers
    /// the Workspace's row count, so the statistics step of the same pass
    /// recounts the Workspace under its lock.
    pub(crate) async fn remove_superseded(
        &self,
        transaction: &mut Transaction<'_>,
        active: &[ActiveGeneration],
    ) -> Result<(), IndexError> {
        let active = active
            .iter()
            .map(ActiveGeneration::generation_id)
            .collect::<HashSet<_>>();
        let superseded = repository::recorded_generations(transaction)
            .await?
            .into_iter()
            .map(|state| state.generation)
            .chain(repository::generations_with_rows(transaction).await?)
            .filter(|generation| !active.contains(&generation.generation_id))
            .collect::<BTreeSet<_>>();
        for generation in &superseded {
            repository::delete_generation_in_workspace(transaction, generation).await?;
        }
        Ok(())
    }
}

/// One Generation's rows, and the search warnings preparing them left.
#[derive(Default)]
pub(crate) struct GenerationRows<'e> {
    rows: Vec<OperationRow<'e>>,
    warnings: Vec<(&'e OperationKey, CatalogDiagnostic)>,
}

impl<'e> GenerationRows<'e> {
    /// Each Operation's search warnings, with its key.
    pub(crate) fn warnings(&self) -> &[(&'e OperationKey, CatalogDiagnostic)] {
        &self.warnings
    }
}

/// The Generation an attempt tried to index, as its state row names it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AttemptedGeneration<'a> {
    workspace_id: &'a WorkspaceId,
    provider_id: &'a ProviderId,
    generation_id: &'a ProviderGenerationId,
    display_name: &'a str,
}

impl<'a> From<&'a GenerationEntries> for AttemptedGeneration<'a> {
    fn from(entries: &'a GenerationEntries) -> Self {
        Self {
            workspace_id: entries.workspace_id(),
            provider_id: entries.provider_id(),
            generation_id: entries.generation_id(),
            display_name: entries.display_name(),
        }
    }
}

impl<'a> From<&'a ActiveGeneration> for AttemptedGeneration<'a> {
    fn from(generation: &'a ActiveGeneration) -> Self {
        Self {
            workspace_id: generation.workspace_id(),
            provider_id: generation.provider_id(),
            generation_id: generation.generation_id(),
            display_name: generation.display_name(),
        }
    }
}

impl AttemptedGeneration<'_> {
    pub(crate) const fn workspace_id(&self) -> &WorkspaceId {
        self.workspace_id
    }

    pub(crate) const fn provider_id(&self) -> &ProviderId {
        self.provider_id
    }

    pub(crate) const fn generation_id(&self) -> &ProviderGenerationId {
        self.generation_id
    }
}

/// Records a failed attempt for the Generation: a `failed` state row with
/// the error and its retry rule under `reconcile_interval`, and no Operation
/// rows. The caller runs this in a fresh transaction after the attempt's
/// transaction rolled back. Rows an earlier attempt wrote go too: a
/// Generation whose state is `failed` counts as not searched, so none of its
/// rows may rank.
pub(crate) async fn record_failed_attempt(
    transaction: &mut Transaction<'_>,
    generation: AttemptedGeneration<'_>,
    error: &IndexError,
    reconcile_interval: Duration,
) -> Result<(), IndexError> {
    repository::lock_workspace(transaction, generation.workspace_id).await?;
    let prior = repository::failed_generation_retry_rule(
        transaction,
        generation.workspace_id,
        generation.generation_id,
    )
    .await?;
    repository::delete_generation_rows(
        transaction,
        generation.workspace_id,
        generation.generation_id,
    )
    .await?;
    let attempted_at = now_unix_nanos()?;
    let state = AttemptState::Failed {
        error: error.to_string(),
        failure: error.failure(),
        retry: RetryRule::after_failure(prior.as_ref(), error, attempted_at, reconcile_interval),
    };
    repository::write_generation_state(
        transaction,
        generation_state(generation, attempted_at, state),
    )
    .await?;
    Ok(())
}

fn generation_state(
    generation: AttemptedGeneration<'_>,
    attempted_at_unix_nanos: i64,
    state: AttemptState,
) -> GenerationState<'_> {
    GenerationState {
        workspace_id: generation.workspace_id,
        provider_id: generation.provider_id,
        generation_id: generation.generation_id,
        display_name: generation.display_name,
        attempted_at_unix_nanos,
        state,
    }
}

fn operation_row<'a>(
    entries: &'a GenerationEntries,
    operation: &OperationEntry<'a>,
) -> Result<OperationRow<'a>, IndexError> {
    let call_path = CallPath::operation(entries.call_root(), operation.relative_path())
        .map_err(|error| IndexError::InvalidEntry(error.to_string()))?;
    let description = [Some(operation.description()), operation.guidance()]
        .into_iter()
        .flatten()
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    Ok(OperationRow {
        workspace_id: entries.workspace_id(),
        provider_id: entries.provider_id(),
        generation_id: entries.generation_id(),
        operation_key: operation.key(),
        call_path,
        qualified_name: operation.relative_path(),
        title: operation.title().unwrap_or_default().to_string(),
        description,
        provider_name: format!("{} {}", entries.call_root().name(), entries.display_name()),
        searchable_text: searchable_text(entries, operation),
        deprecated: operation.deprecated(),
    })
}

/// The lowest-weight text: the words of the tags, of every declaration path
/// with the values the schema lists, and of the compact identifier variants
/// of the name-like fields, each once, as [`DistinctWords`] keeps them. The
/// deprecation flag is a column, never text. Raise [`INDEXER_REVISION`]
/// when this text changes.
fn searchable_text(entries: &GenerationEntries, operation: &OperationEntry<'_>) -> String {
    let names = [
        operation.relative_path(),
        operation.title().unwrap_or_default(),
        entries.call_root().name(),
        entries.display_name(),
    ];
    let variants = names
        .into_iter()
        .filter_map(compact_identifier_variant)
        .collect::<Vec<_>>();
    let mut words = DistinctWords::default();
    for tag in operation.tags() {
        words.push(tag);
    }
    for field in operation.declaration_fields() {
        words.push_field(field.path().as_str(), field.text());
    }
    for variant in &variants {
        words.push(variant);
    }
    words.into_text()
}
