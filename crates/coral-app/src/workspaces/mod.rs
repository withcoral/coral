pub(crate) mod manager;
pub(crate) mod model;
pub(crate) mod name;
pub(crate) mod store;

#[expect(unused_imports, reason = "used in next stack PR")]
pub(crate) use manager::WorkspaceManager;
pub(crate) use model::{DeletedWorkspace, WorkspaceRecord};
pub use name::DEFAULT_WORKSPACE_ID;
pub(crate) use name::WorkspaceName;
pub(crate) use store::WorkspaceStore;
