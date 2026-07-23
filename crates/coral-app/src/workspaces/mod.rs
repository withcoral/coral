pub(crate) mod manager;
pub(crate) mod model;
pub(crate) mod name;
pub(crate) mod paths;
pub(crate) mod pool_registries;
pub(crate) mod service;

pub(crate) use manager::WorkspaceManager;
pub(crate) use model::{
    DeletedWorkspace, WorkspaceLifecycleLock, WorkspaceLifecycleRevision, WorkspaceRecord,
};
pub use name::DEFAULT_WORKSPACE_ID;
pub(crate) use name::WorkspaceName;
pub(crate) use paths::WorkspacePaths;
pub(crate) use pool_registries::WorkspacePoolRegistries;
pub(crate) use service::WorkspaceService;
