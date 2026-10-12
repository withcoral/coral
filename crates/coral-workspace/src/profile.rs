//! Narrow Workspace-authorized profiles, never authentication records or a directory.
//! Verified login ingestion joins the authentication transaction. Profile refresh and
//! email grants share the Organization administration hold before any User holds,
//! so a new profile or eligibility change cannot introduce an unseen email collision.
use crate::profile_repository as repository;
use coral_db::Transaction;

use crate::{
    AuthenticatedUser, Error, OrganizationModule, UserId, WorkspaceAction, WorkspaceId,
    WorkspaceModule, WorkspaceRole,
};

/// Mutable, non-authorizing presentation attributes. No authentication evidence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UserProfile {
    /// Stable Coral identity, never derived from email.
    pub user_id: UserId,
    /// Trusted profile name, distinct from the legacy authentication display claim.
    pub name: Option<String>,
    /// Provider spelling, including unverified addresses when display is authorized.
    pub email: Option<String>,
    /// The implicit local User has no invented email.
    pub local: bool,
}

/// Separately verified upstream claims. The host supplies these only after token validation.
/// Intentionally not Debug: raw login evidence must not enter telemetry.
#[derive(Default)]
pub struct VerifiedUserProfile<'a> {
    pub(super) name: Option<&'a str>,
    pub(super) email: Option<&'a str>,
    pub(super) email_verified: Option<bool>,
    pub(super) hosted_domain: Option<&'a str>,
}
impl<'a> VerifiedUserProfile<'a> {
    /// Carries signed profile claims and their explicit verification evidence.
    #[must_use]
    pub const fn new(
        name: Option<&'a str>,
        email: Option<&'a str>,
        email_verified: Option<bool>,
        hosted_domain: Option<&'a str>,
    ) -> Self {
        Self {
            name,
            email,
            email_verified,
            hosted_domain,
        }
    }
}

impl OrganizationModule {
    /// Serializes trusted profile ingestion with Organization administration and email grants.
    /// Must precede authentication-directory and User row writes; never commits.
    /// # Errors
    /// Returns persistence failures.
    pub async fn hold_profile_updates_in_transaction(
        tx: &mut Transaction<'_>,
    ) -> Result<(), Error> {
        crate::organization::hold_administration(tx).await
    }

    /// Refreshes safe profile attributes in the verified-login transaction, not authority.
    /// The caller must first hold profile updates. Older login completions are ignored.
    /// # Errors
    /// Returns persistence failures; never commits independently.
    pub async fn refresh_profile_in_transaction(
        tx: &mut Transaction<'_>,
        user: &UserId,
        profile: VerifiedUserProfile<'_>,
        observed_at: i64,
    ) -> Result<(), Error> {
        repository::upsert(
            tx,
            user,
            &profile,
            profile.email.map(email_key),
            observed_at,
        )
        .await
    }
}

fn email_key(email: &str) -> String {
    email.trim().to_ascii_lowercase()
}

pub(super) async fn read_profile(
    tx: &mut Transaction<'_>,
    user: &UserId,
) -> Result<UserProfile, Error> {
    let local = user == crate::local_organization_owner().user_id();
    let row = repository::find(tx, user).await?;
    let (name, email) = row.map(|row| (row.name, row.email)).unwrap_or_default();
    Ok(UserProfile {
        user_id: user.clone(),
        name: if local {
            Some("Local user".into())
        } else {
            name
        },
        email: if local { None } else { email },
        local,
    })
}

async fn eligible_matches(tx: &mut Transaction<'_>, email: &str) -> Result<UserId, Error> {
    let mut matched = None;
    for id in repository::verified_candidates(tx, email).await? {
        match crate::organization::current_user(tx, &id).await {
            Ok(user) if user.organization_role().is_some() => {
                if matched.is_some() {
                    return Err(Error::AmbiguousEmail);
                }
                matched = Some(id);
            }
            Ok(_) | Err(Error::PermissionDenied(_)) => {}
            Err(error) => return Err(error),
        }
    }
    matched.ok_or(Error::MembershipNotFound)
}

pub(super) async fn resolve_email(tx: &mut Transaction<'_>, email: &str) -> Result<UserId, Error> {
    let email = email_key(email);
    if email.is_empty() || email.len() > 320 {
        return Err(Error::MembershipNotFound);
    }
    eligible_matches(tx, &email).await
}

impl WorkspaceModule {
    /// Projects a profile only through this Workspace's People or retained Agent attribution.
    /// Members can see themselves and the designated Owners, never other Members.
    /// # Errors
    /// Conceals all other targets, even when their User ID is known.
    pub async fn inspect_user_profile(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
        user: &UserId,
    ) -> Result<UserProfile, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let profile = self
            .inspect_user_profile_in_transaction(&mut tx, actor, workspace, user)
            .await?;
        tx.commit().await?;
        Ok(profile)
    }

    /// Joins authorized retained evidence to a currently visible profile in one snapshot.
    /// The same People visibility rules apply; stable attribution alone grants no profile access.
    /// # Errors
    /// Conceals inaccessible targets and returns persistence failures.
    pub async fn inspect_user_profile_in_transaction(
        &self,
        tx: &mut Transaction<'_>,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
        user: &UserId,
    ) -> Result<UserProfile, Error> {
        let evidence = self
            .check_in_transaction(tx, actor, workspace, WorkspaceAction::Inspect)
            .await?;
        let designated =
            crate::repository::membership_role_in_transaction(tx, workspace, user).await?;
        let designated_owner = designated == Some(WorkspaceRole::Owner);
        let owner = evidence.effective_role() == WorkspaceRole::Owner;
        let creator = if owner {
            crate::agent_repository::has_creator(tx, workspace, user).await?
        } else {
            false
        };
        if actor.user_id() != user
            && !designated_owner
            && !(owner && (designated.is_some() || creator))
        {
            return Err(Error::MembershipNotFound);
        }
        read_profile(tx, user).await
    }
}
