/// Public-safe user directory entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UserView {
    pub(crate) user_id: String,
    pub(crate) display_name: Option<String>,
}

/// The authenticated human user's public identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CurrentUser {
    pub(crate) user: UserView,
}
