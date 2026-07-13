//! App-owned identity instance domain types.

pub(crate) mod manager;
pub(crate) mod model;
pub(crate) mod runtime;
pub(crate) mod service;
pub(crate) mod workspace_service;

pub(crate) use service::IdentityService;
pub(crate) use workspace_service::WorkspaceIdentityService;
