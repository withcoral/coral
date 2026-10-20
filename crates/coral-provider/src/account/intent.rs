//! Accepted Account authorization evidence. Initial connection and explicit replacement
//! are distinct intents; neither completion nor a callback infers intent from today's
//! credentials. Setup authorization remains provisional until Publication.
use super::ProviderAccountId;
use crate::ProviderId;
use coral_workspace::{PrincipalId, UserId};

#[derive(Clone, PartialEq, Eq)]
pub(super) enum AuthorizationTarget {
    Shared,
    User(ProviderId),
    Agent {
        target_provider: ProviderId,
        agent: PrincipalId,
    },
    Reconnect(ProviderAccountId),
}

#[derive(Clone)]
pub(crate) enum Caller {
    User(UserId),
    Agent(PrincipalId),
}

impl Caller {
    /// The caller scope a Principal's own Accounts live in.
    pub(crate) fn of_principal(
        principal: &coral_workspace::Principal,
    ) -> Result<Self, crate::Error> {
        Ok(match principal.kind() {
            coral_workspace::PrincipalKind::User => {
                Self::User(UserId::parse(principal.id().as_str())?)
            }
            coral_workspace::PrincipalKind::Agent => Self::Agent(principal.id().clone()),
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ConnectionIntent {
    Initial,
    Replace,
}

pub(super) struct AuthorizationIntent {
    pub(super) account: ProviderAccountId,
    pub(super) authorization_version: String,
    pub(super) provider_account_setting_version: i64,
    pub(super) kind: ConnectionIntent,
    pub(super) caller: Option<Caller>,
}
impl AuthorizationIntent {
    pub(super) fn caller(&self) -> Option<Caller> {
        self.caller.clone()
    }
    pub(super) fn is_user(&self) -> bool {
        matches!(self.caller, Some(Caller::User(_)))
    }
}

/// Exact principal whose open workflows share one pending authorization cap.
pub(super) enum AuthorizationPrincipal {
    User(UserId),
    Agent(PrincipalId),
    Administrative(UserId),
}
