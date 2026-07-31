//! Query orchestration and transport adapters.

pub(crate) mod attribution;
pub(crate) mod extensions;
pub(crate) mod input_resolver;
pub(crate) mod manager;
pub(crate) mod service;

pub(crate) use attribution::QueryAttribution;
#[expect(
    unused_imports,
    reason = "native fanout consumes this internal execution seam in the next stacked PR"
)]
pub(crate) use manager::{
    ExecuteSelectedTableFunction, SelectedTableFunctionExecution,
    SelectedTableFunctionExecutionError, SelectedTableFunctionFailureKind,
};
