//! Function lifecycle and inventory workflow.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "function lifecycle service and CLI callers land upstack in the split stack"
    )
)]

pub(crate) mod manager;
pub(crate) mod model;
mod runtime;
mod store;
mod validation;

pub(crate) use model::FunctionName;
