//! Function lifecycle and inventory workflow.
#![expect(
    dead_code,
    reason = "function inventory and artifact stores are introduced before runtime and service callers in the split app stack"
)]

pub(crate) mod manager;
pub(crate) mod model;
mod store;

pub(crate) use model::FunctionName;
