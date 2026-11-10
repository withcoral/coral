//! Usage data persistence: the install row, the stored reports, and the
//! counts read from other modules' tables. Callers own authorization,
//! transactions and outcome selection.

pub(crate) mod activity;
pub(crate) mod install;
