//! Workspace membership roles.
//!
//! A caller's role is the whole of their workspace-scoped authority, and the
//! set is closed: a workspace has owners and members and nothing else. The
//! storage encoding lives here so persistence and authorization share one
//! spelling of that set instead of each carrying its own string literals.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the role lands with its persistence, before the authorizer and RPCs that read it"
    )
)]

/// One caller's role in one workspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MemberRole {
    Owner,
    Member,
}

impl MemberRole {
    /// Encodes the role for the `workspace_members.role` column.
    ///
    /// These literals are pinned by the `workspace_members_role_valid` check
    /// constraint in `0006_workspace_access_control.sql`; changing one is a
    /// migration, not an edit here.
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::Member => "member",
        }
    }

    /// Decodes one stored role, recognizing nothing outside the closed set.
    ///
    /// Returning `None` rather than a default is what lets the read paths fail
    /// closed: the check constraint rejects unknown roles on the way in, so a
    /// value that reaches here came from an out-of-band write or a newer
    /// Coral, and an unknown authority must not be softened into `Member`.
    pub(crate) fn from_storage_str(value: &str) -> Option<Self> {
        match value {
            "owner" => Some(Self::Owner),
            "member" => Some(Self::Member),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::MemberRole;

    #[test]
    fn member_role_round_trips_through_its_storage_encoding() {
        for role in [MemberRole::Owner, MemberRole::Member] {
            assert_eq!(
                MemberRole::from_storage_str(role.as_storage_str()),
                Some(role)
            );
        }
    }

    #[test]
    fn member_role_recognizes_no_value_outside_the_closed_set() {
        for value in ["admin", "Owner", "owners", "", "0"] {
            assert_eq!(
                MemberRole::from_storage_str(value),
                None,
                "'{value}' must not decode to a role"
            );
        }
    }
}
