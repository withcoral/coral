pub(crate) mod authorization;
pub(crate) mod manager;
pub(crate) mod member;
pub(crate) mod model;
pub(crate) mod name;
pub(crate) mod paths;
pub(crate) mod pool_registry;
pub(crate) mod service;

pub(crate) use manager::WorkspaceManager;
pub(crate) use member::MemberRole;
pub(crate) use model::{
    DeletedWorkspace, WorkspaceLifecycleLock, WorkspaceLifecycleReadLease,
    WorkspaceLifecycleRevision, WorkspaceRecord,
};
pub use name::DEFAULT_WORKSPACE_ID;
pub(crate) use name::WorkspaceName;
pub(crate) use paths::WorkspacePaths;
pub(crate) use pool_registry::WorkspacePoolRegistry;
pub(crate) use service::WorkspaceService;
