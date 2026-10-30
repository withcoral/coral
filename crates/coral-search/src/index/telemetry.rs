//! Read-only projection snapshots. No database work runs from metric callbacks,
//! and observation does not wait for the index writer mutex.
use super::SearchIndex;
use coral_telemetry::metric;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Default)]
pub(super) struct IndexTelemetry {
    observation: Mutex<Option<metric::SearchObservation>>,
    snapshot: Mutex<Option<Snapshot>>,
}
struct Snapshot {
    backlog: metric::SearchBacklog,
    sampled: Instant,
}

// Sampling cadence is independent of reconciliation. Tests shorten only the
// clock so the same background task can exercise snapshot transitions.
#[cfg(not(test))]
const SAMPLE_INTERVAL: Duration = Duration::from_secs(30);
#[cfg(test)]
const SAMPLE_INTERVAL: Duration = Duration::from_millis(50);

const READ_LIMIT: Duration = Duration::from_secs(30);

/// An aborted or panicked observer cannot continue reporting its last value.
struct ObservationLifetime(Arc<IndexTelemetry>);
impl Drop for ObservationLifetime {
    fn drop(&mut self) {
        let _removed = self
            .0
            .observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        *self
            .0
            .snapshot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

impl SearchIndex {
    pub(super) async fn observe_backlog_loop(&self) {
        let _lifetime = ObservationLifetime(self.telemetry.clone());
        let mut starts = tokio::time::interval(SAMPLE_INTERVAL);
        starts.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            starts.tick().await;
            self.observe_backlog().await;
        }
    }

    pub(super) async fn observe_backlog(&self) {
        {
            let mut registered = self
                .telemetry
                .observation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if registered.is_none() {
                let state = std::sync::Arc::downgrade(&self.telemetry);
                let Some(observation) = metric::observe_search_backlog(move || {
                    let state = state.upgrade()?;
                    let snapshot = state
                        .snapshot
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let snapshot = snapshot.as_ref()?;
                    let age = snapshot.sampled.elapsed();
                    let mut backlog = snapshot.backlog;
                    if let Some(first) = &mut backlog.first_visibility {
                        first.oldest_age =
                            first.oldest_age.map(|oldest| oldest.saturating_add(age));
                    }
                    backlog.sample_age = age;
                    Some(backlog)
                }) else {
                    return;
                };
                *registered = Some(observation);
            }
        }
        let read = async {
            let mut tx = self.database.begin_read_snapshot().await?;
            // Age the snapshot from the start of its reads, not the end of
            // potentially slow scans.
            let sampled = Instant::now();
            let observed_at = crate::unix_now_nanos().ok_or(crate::indexer::IndexError::Clock)?;
            let generations = coral_provider::active_generations_in_transaction(&mut tx).await?;
            let functions =
                coral_functions::saved_function_index_revisions_in_transaction(&mut tx).await?;
            let backlog =
                crate::indexer::projection_backlog(&mut tx, &generations, &functions).await?;
            let first_visibility = if let Some(missing) = &backlog.first_visibility {
                let publications =
                    coral_catalog::generation_publication_times_in_transaction(&mut tx, missing)
                        .await
                        .map_err(coral_provider::Error::from)?;
                if publications.len() != missing.len() {
                    return Err(crate::indexer::IndexError::Corrupt(
                        "catalog has no publication time",
                    ));
                }
                Some(metric::FirstVisibilityBacklog {
                    waiting: u64::try_from(missing.len()).unwrap_or(u64::MAX),
                    oldest_age: publications
                        .values()
                        .min()
                        .and_then(|at| observed_at.checked_sub(*at))
                        .and_then(|age| u64::try_from(age).ok())
                        .map(Duration::from_nanos),
                })
            } else {
                None
            };
            tx.rollback().await?;
            Ok::<_, crate::indexer::IndexError>(Snapshot {
                backlog: metric::SearchBacklog {
                    generations: backlog.catalogs,
                    functions: backlog.functions,
                    first_visibility,
                    sample_age: Duration::ZERO,
                },
                sampled,
            })
        };
        // A failed read is unknown, never a zero backlog or a stale successful sample.
        let snapshot = tokio::time::timeout(READ_LIMIT, read)
            .await
            .ok()
            .and_then(Result::ok);
        *self
            .telemetry
            .snapshot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = snapshot;
    }
}
