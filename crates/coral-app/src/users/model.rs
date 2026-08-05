use crate::workspaces::WorkspaceName;

/// Public-safe user directory entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UserView {
    pub(crate) user_id: String,
    pub(crate) display_name: Option<String>,
}

/// The authenticated human user and their app-derived personal workspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CurrentUser {
    pub(crate) user: UserView,
    pub(crate) default_workspace: WorkspaceName,
}
