//! Observed-values collection and governance for Universal Search.

mod collector;
mod governance;
mod live_scope;
mod policy;
pub(crate) mod provider;
mod publisher;
mod ranking;
mod sensitive;
mod source_scope;
mod sqlite_projection;
mod sqlite_queue;
mod sqlite_store;
mod writer;

pub(crate) use live_scope::{ObservedValuesLiveScopeLoad, ObservedValuesLiveScopeLoader};
pub(crate) use policy::{
    ObservedValuesLiveScope, ObservedValuesLiveScopeLoadFailure, ObservedValuesRetrievalPolicy,
};
pub(crate) use publisher::{SearchObservationHandle, SearchObservationSource};
pub(crate) use sqlite_projection::ObservedValuesDrainBudget;

#[cfg(test)]
pub(crate) use sqlite_queue::{ObservedValuesQueueJob, ObservedValuesSurfaceKind};
#[cfg(test)]
pub(crate) use sqlite_store::SqliteObservedValuesStore;
