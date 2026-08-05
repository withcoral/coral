#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "workspace membership APIs are wired to production consumers in later milestones"
    )
)]

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MemberRole {
    Owner,
    Member,
}

impl MemberRole {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::Member => "member",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "owner" => Some(Self::Owner),
            "member" => Some(Self::Member),
            _ => None,
        }
    }
}
