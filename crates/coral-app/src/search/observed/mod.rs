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
pub(crate) use publisher::SearchObservationHandle;
pub(crate) use sqlite_projection::ObservedValuesDrainBudget;
pub(crate) use sqlite_store::{ObservedValuesClearResult, clear_observed_source_in_transaction};

#[cfg(test)]
pub(crate) use sqlite_queue::{ObservedValuesQueueJob, ObservedValuesSurfaceKind};
#[cfg(test)]
pub(crate) use sqlite_store::SqliteObservedValuesStore;
