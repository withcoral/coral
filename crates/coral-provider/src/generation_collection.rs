//! Deletes the Provider Generations that nothing uses any more.
use std::time::Duration;

use sea_query::{ColumnRef, SimpleExpr};

use crate::{Error, ProviderModule, repository};

/// How long a Generation stays after a pass first finds it unused. It covers
/// a Run that resolved an Operation while the Generation was active but has
/// not yet recorded its Invocation, which a Run Owner does within a few
/// worker leases or interrupts the Run.
pub const GENERATION_RETIREMENT_GRACE: Duration = Duration::from_mins(10);

/// Retired Generations one pass deletes, each in its own transaction. A
/// pass leaves the rest to the next.
const DELETIONS_PER_PASS: u64 = 32;

/// What, besides a Provider's active selection, still uses a Generation: a
/// condition on the Generation id column, such as an Invocation of a running
/// Run that names it.
pub type GenerationHold = fn(ColumnRef) -> SimpleExpr;

/// What one collection pass did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GenerationCollection {
    /// Generations this pass first found unused.
    pub retired: u64,
    /// Generations this pass deleted.
    pub deleted: u64,
}

impl ProviderModule {
    /// Runs one collection pass at `now`. A Generation is unused when no
    /// Provider selects it and `held` does not hold it. The pass marks each
    /// newly unused Generation, then deletes each one that a pass found
    /// unused at least [`GENERATION_RETIREMENT_GRACE`] ago and that is still
    /// unused, with its Operations, shared types and search rows.
    ///
    /// # Errors
    ///
    /// Returns a persistence failure. Deletions committed before it stand.
    pub async fn collect_generations(
        &self,
        held: GenerationHold,
        now_unix_nanos: i64,
    ) -> Result<GenerationCollection, Error> {
        let in_use = repository::generation_in_use(held);
        let mut collection = GenerationCollection::default();
        let mut tx = self.database.begin().await?;
        collection.retired = coral_catalog::retire_unused_generations_in_transaction(
            &mut tx,
            &in_use,
            now_unix_nanos,
        )
        .await?;
        tx.commit().await?;

        let grace = i64::try_from(GENERATION_RETIREMENT_GRACE.as_nanos()).unwrap_or(i64::MAX);
        let retired_by = now_unix_nanos.saturating_sub(grace);
        let mut tx = self.database.begin_read_snapshot().await?;
        let due = coral_catalog::retired_generations_in_transaction(
            &mut tx,
            &in_use,
            retired_by,
            DELETIONS_PER_PASS,
        )
        .await?;
        tx.rollback().await?;
        for generation in &due {
            let mut tx = self.database.begin().await?;
            if coral_catalog::delete_retired_generation_in_transaction(
                &mut tx, generation, &in_use, retired_by,
            )
            .await?
            {
                tx.commit().await?;
                collection.deleted += 1;
            } else {
                tx.rollback().await?;
            }
        }
        Ok(collection)
    }
}
