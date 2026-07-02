//! Observed-values collection and governance for Universal Search.

mod collector;
mod publisher;
mod sensitive;
mod source_scope;
mod sqlite_queue;
mod sqlite_store;
mod writer;

pub(crate) use publisher::SearchObservationHandle;
