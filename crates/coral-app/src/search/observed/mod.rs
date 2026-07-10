//! Observed-values collection and governance for Universal Search.

mod collector;
mod governance;
mod projection;
mod publisher;
mod sensitive;
mod source_scope;
mod sqlite_projection;
mod sqlite_queue;
mod sqlite_store;
mod writer;

pub(crate) use projection::ObservedValuesProjection;
pub(crate) use publisher::{SearchObservationHandle, SearchObservationSource};
pub(crate) use sqlite_projection::ObservedValuesDrainBudget;

#[cfg(test)]
pub(crate) use sqlite_queue::{ObservedValuesQueueJob, ObservedValuesSurfaceKind};
#[cfg(test)]
pub(crate) use sqlite_store::SqliteObservedValuesStore;
