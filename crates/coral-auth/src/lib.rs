//! Coral User access-token issuance, credential-to-Principal authentication, and
//! stable User identity provisioning for verified OIDC logins.
//!
//! This private domain Module owns the durable mapping from one verified OIDC
//! identity to one opaque Coral [`UserId`]. The authorization-server Adapter
//! validates the upstream protocol and supplies the resulting identity; this
//! Module performs transactional identity and initial Membership provisioning
//! without exposing database rows to application composition. Organization role
//! and disablement rules remain owned by `coral-workspace::OrganizationModule`.
//!
//! # Interface
//!
//! - [`UserDirectory::provision_login`] creates or refreshes one verified login
//!   and returns its stable Coral User identity.
//! - [`SessionTokenIssuer`] issues User credentials from resolved configuration.
//! - [`PrincipalAuthentication::authenticate`] verifies a presented credential for
//!   an exact caller-supplied audience and Workspace, returning canonical evidence
//!   with current Organization or Agent Registration authority. JWT machinery is
//!   private; Adapters own URL projection and protocol error responses.
//!
//! # Invariants
//!
//! A first login mints one random Coral User identity. Concurrent first logins
//! converge on that identity, and later logins preserve it. A subject already
//! bound to another configured issuer is never rebound automatically. Failed
//! provisioning commits no directory or Membership change. Only the explicitly
//! configured bootstrap issuer/subject selects an initial Owner; ordinary login
//! provisions a Member and cannot reset a current role or disabled state.
//! Issuer, subject, and display name remain private authentication data.
//! Authentication never grants Workspace or resource access: domain Modules still
//! authorize each operation and recheck authority in their transactions. Missing,
//! invalid, expired, or disabled credentials fail closed with bounded errors;
//! persistence failures report unavailability without exposing token material.

mod audience;
mod authentication;
pub mod authorization_codes;
pub mod oauth_clients;
pub use audience::PrivateAudience;
mod repository;
mod session;

pub use authentication::{AuthenticationError, PrincipalAuthentication, WorkspaceFixedBy};
pub use session::{IssuedAccessToken, SessionTokenError, SessionTokenIssuer};

use std::sync::Arc;

use coral_db::Database;
pub use coral_workspace::UserId;
use coral_workspace::{
    AuthenticatedUser, ChangeContext, OrganizationModule, OrganizationRole, TraceId,
};

/// External credentials may identify a User, but never the implicit local Owner.
fn external_user_id(value: &str) -> Result<UserId, ()> {
    let id = UserId::parse(value).map_err(|_error| ())?;
    if &id == coral_workspace::local_organization_owner().user_id() {
        return Err(());
    }
    Ok(id)
}

/// One verified OIDC identity supplied by the authorization-server Adapter.
pub struct VerifiedOidcLogin<'a> {
    issuer: &'a str,
    subject: &'a str,
    display_name: Option<&'a str>,
    profile: coral_workspace::VerifiedUserProfile<'a>,
    profile_observed_at_unix_nanos: i64,
    occurred_at_unix_nanos: i64,
}

impl<'a> VerifiedOidcLogin<'a> {
    /// Builds the persistence input for one successfully verified login.
    #[must_use]
    pub const fn new(
        issuer: &'a str,
        subject: &'a str,
        display_name: Option<&'a str>,
        occurred_at_unix_nanos: i64,
    ) -> Self {
        Self {
            issuer,
            subject,
            display_name,
            profile: coral_workspace::VerifiedUserProfile::new(None, None, None, None),
            profile_observed_at_unix_nanos: occurred_at_unix_nanos,
            occurred_at_unix_nanos,
        }
    }

    /// Supplies separately configured trusted name, email, and verification claims.
    #[must_use]
    pub fn with_profile(mut self, profile: coral_workspace::VerifiedUserProfile<'a>) -> Self {
        self.profile = profile;
        self
    }

    /// Pins profile freshness to the verified token's issuance time, independently
    /// of the callback's completion/event time. Older completions cannot win.
    #[must_use]
    pub const fn with_profile_observed_at(mut self, observed_at_unix_nanos: i64) -> Self {
        self.profile_observed_at_unix_nanos = observed_at_unix_nanos;
        self
    }
}

/// Result of provisioning one verified login.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoginProvisioning {
    /// The stable Coral User identity for this upstream login.
    Provisioned(UserId),
    /// The subject is already bound to another configured issuer.
    IssuerMismatch,
    /// Coral-side disablement persists across otherwise valid logins.
    Disabled,
}

/// A failure to provision a verified login.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Persistent authentication data violated the Module's invariants.
    #[error("authentication directory contains corrupt data: {0}")]
    CorruptData(String),
    /// The persistence Adapter failed.
    #[error(transparent)]
    Database(#[from] coral_db::Error),
    /// Organization provisioning failed; identity and Membership roll back together.
    #[error("Organization Membership provisioning failed")]
    Organization(#[from] coral_workspace::Error),
}

/// Transactional directory for stable Coral User identities.
#[derive(Clone)]
pub struct UserDirectory {
    database: Arc<Database>,
    bootstrap_owner: Option<(String, String)>,
}

impl UserDirectory {
    /// Builds the Module over initialized persistence mechanics.
    #[must_use]
    pub const fn new(database: Arc<Database>) -> Self {
        Self {
            database,
            bootstrap_owner: None,
        }
    }

    /// Selects the exact verified issuer and subject eligible for initial Owner provisioning.
    /// This operator setting is never taken from client metadata or an email address.
    /// It has no effect on an existing Membership's role or disabled state.
    #[must_use]
    pub fn with_bootstrap_owner(mut self, issuer: &str, subject: &str) -> Self {
        self.bootstrap_owner = Some((issuer.to_owned(), subject.to_owned()));
        self
    }

    /// Creates or refreshes the directory mapping for one verified login.
    ///
    /// # Errors
    ///
    /// Returns a persistence or corrupt-data error. Failure commits no change.
    pub async fn provision_login(
        &self,
        login: VerifiedOidcLogin<'_>,
    ) -> Result<LoginProvisioning, Error> {
        let mut transaction = self.database.begin().await?;
        OrganizationModule::hold_profile_updates_in_transaction(&mut transaction).await?;
        let candidate_user_id = UserId::parse(&uuid::Uuid::new_v4().to_string())?;
        let identity =
            repository::upsert_login(&mut transaction, &candidate_user_id, &login).await?;
        if identity.issuer != login.issuer {
            transaction.rollback().await?;
            return Ok(LoginProvisioning::IssuerMismatch);
        }
        let user_id = identity.user_id;

        let initial_role = if self
            .bootstrap_owner
            .as_ref()
            .is_some_and(|(issuer, subject)| issuer == login.issuer && subject == login.subject)
        {
            OrganizationRole::Owner
        } else {
            OrganizationRole::Member
        };
        let membership = OrganizationModule::provision_login_in_transaction(
            &mut transaction,
            &ChangeContext::new(
                AuthenticatedUser::organization_member(user_id.clone(), initial_role),
                login.occurred_at_unix_nanos,
                TraceId::current_or_new(),
            ),
        )
        .await?;
        OrganizationModule::refresh_profile_in_transaction(
            &mut transaction,
            &user_id,
            login.profile,
            login.profile_observed_at_unix_nanos,
        )
        .await?;
        if !membership.enabled() {
            transaction.commit().await?;
            return Ok(LoginProvisioning::Disabled);
        }

        transaction.commit().await?;
        Ok(LoginProvisioning::Provisioned(user_id))
    }
}

#[cfg(test)]
mod tests {
    use super::{LoginProvisioning, UserDirectory, VerifiedOidcLogin};
    use coral_test_support::TestDatabase;

    #[tokio::test]
    async fn trusted_profile_refresh_preserves_identity_and_rejects_stale_login_updates() {
        let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
        let directory = UserDirectory::new(fixture.database());
        let user = provisioned(
            directory
                .provision_login(
                    login("https://issuer.test", "ada", Some("not-a-profile-name"), 10)
                        .with_profile(coral_workspace::VerifiedUserProfile::new(
                            Some("Ada"),
                            Some("Ada@Example.test"),
                            Some(true),
                            None,
                        )),
                )
                .await
                .unwrap(),
        );
        let organization = coral_workspace::OrganizationModule::new(fixture.database());
        let actor = organization.resolve_user_authority(&user).await.unwrap();
        let workspaces = coral_workspace::WorkspaceModule::new(fixture.database());
        let context = coral_workspace::ChangeContext::new(
            actor.clone(),
            11,
            coral_workspace::TraceId::current_or_new(),
        );
        let workspace = workspaces
            .create(
                &context,
                coral_workspace::WorkspaceDisplayName::parse("Profiles").unwrap(),
            )
            .await
            .unwrap();
        let profile = workspaces
            .inspect_user_profile(&actor, workspace.id(), &user)
            .await
            .unwrap();
        assert_eq!(profile.name.as_deref(), Some("Ada"));
        assert_eq!(profile.email.as_deref(), Some("Ada@Example.test"));
        assert_eq!(
            provisioned(
                directory
                    .provision_login(
                        login("https://issuer.test", "ada", None, 30)
                            .with_profile(coral_workspace::VerifiedUserProfile::new(
                                Some("Stale"),
                                None,
                                None,
                                None
                            ))
                            .with_profile_observed_at(5)
                    )
                    .await
                    .unwrap()
            ),
            user
        );
        assert_eq!(
            workspaces
                .inspect_user_profile(&actor, workspace.id(), &user)
                .await
                .unwrap(),
            profile
        );
    }

    /// Email is profile data, never identity: a verified login that claims
    /// another User's address gets its own User and leaves the other intact.
    #[tokio::test]
    async fn a_colliding_verified_email_neither_overwrites_nor_transfers_identity() {
        use coral_workspace::{
            ChangeContext, OrganizationModule, TraceId, VerifiedUserProfile, WorkspaceDisplayName,
            WorkspaceModule, WorkspaceRole,
        };
        let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
        let directory = UserDirectory::new(fixture.database())
            .with_bootstrap_owner("https://issuer.test", "owner");
        let verified = |email| VerifiedUserProfile::new(None, Some(email), Some(true), None);
        let owner = provisioned(
            directory
                .provision_login(login("https://issuer.test", "owner", None, 1))
                .await
                .unwrap(),
        );
        let alice = provisioned(
            directory
                .provision_login(
                    login("https://issuer.test", "alice", None, 2)
                        .with_profile(verified("alice@example.test")),
                )
                .await
                .unwrap(),
        );
        let bob = provisioned(
            directory
                .provision_login(
                    login("https://issuer.test", "bob", None, 3)
                        .with_profile(verified("alice@example.test")),
                )
                .await
                .unwrap(),
        );
        assert_ne!(bob, alice);

        let organization = OrganizationModule::new(fixture.database());
        let owner = organization.resolve_user_authority(&owner).await.unwrap();
        let workspaces = WorkspaceModule::new(fixture.database());
        let context = ChangeContext::new(owner.clone(), 10, TraceId::current_or_new());
        let workspace = workspaces
            .create(&context, WorkspaceDisplayName::parse("Collisions").unwrap())
            .await
            .unwrap();
        workspaces
            .set_membership(
                &context,
                workspace.id(),
                &organization.resolve_user_authority(&alice).await.unwrap(),
                WorkspaceRole::Member,
            )
            .await
            .unwrap();
        let profile = workspaces
            .inspect_user_profile(&owner, workspace.id(), &alice)
            .await
            .unwrap();
        assert_eq!(profile.user_id, alice);
        assert_eq!(profile.email.as_deref(), Some("alice@example.test"));
        assert_eq!(
            provisioned(
                directory
                    .provision_login(
                        login("https://issuer.test", "alice", None, 20)
                            .with_profile(verified("alice@example.test")),
                    )
                    .await
                    .unwrap()
            ),
            alice
        );
    }

    #[tokio::test]
    async fn verified_login_provisions_membership_and_cannot_undo_disablement() {
        let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
        let directory = UserDirectory::new(fixture.database())
            .with_bootstrap_owner("https://issuer.test", "owner");
        let owner_id = provisioned(
            directory
                .provision_login(login("https://issuer.test", "owner", None, 1))
                .await
                .unwrap(),
        );
        let member_id = provisioned(
            directory
                .provision_login(login("https://issuer.test", "member", None, 1))
                .await
                .unwrap(),
        );
        let organization = coral_workspace::OrganizationModule::new(fixture.database());
        let owner = organization
            .resolve_user_authority(&owner_id)
            .await
            .unwrap();
        assert_eq!(
            owner.organization_role(),
            Some(coral_workspace::OrganizationRole::Owner)
        );
        assert_eq!(
            organization
                .resolve_user_authority(&member_id)
                .await
                .unwrap()
                .organization_role(),
            Some(coral_workspace::OrganizationRole::Member)
        );
        let change = coral_workspace::ChangeContext::new(
            owner,
            2,
            coral_workspace::TraceId::parse(&"a".repeat(32)).unwrap(),
        );
        organization
            .set_enabled(&change, &member_id, 1, false)
            .await
            .unwrap();
        assert_eq!(
            directory
                .provision_login(login("https://issuer.test", "member", None, 3))
                .await
                .unwrap(),
            LoginProvisioning::Disabled
        );
    }

    #[tokio::test]
    async fn login_provisioning_contract_holds_for_selected_database() {
        let fixture = TestDatabase::new(coral_db::migration_plan())
            .await
            .expect("create authentication test database");
        let directory = UserDirectory::new(fixture.database());

        let first = provisioned(
            directory
                .provision_login(login("https://issuer.test", "subject", Some("Ada"), 10))
                .await
                .expect("first login"),
        );
        let refreshed = provisioned(
            directory
                .provision_login(login(
                    "https://issuer.test",
                    "subject",
                    Some("Ada Lovelace"),
                    20,
                ))
                .await
                .expect("later login"),
        );
        let stale = provisioned(
            directory
                .provision_login(login(
                    "https://issuer.test",
                    "subject",
                    Some("Stale Name"),
                    5,
                ))
                .await
                .expect("out-of-order login"),
        );
        assert_eq!(refreshed, first);
        assert_eq!(stale, first);

        assert_eq!(
            directory
                .provision_login(login("https://other.test", "subject", None, 30))
                .await
                .expect("issuer mismatch is a domain result"),
            LoginProvisioning::IssuerMismatch,
        );
        assert_eq!(
            provisioned(
                directory
                    .provision_login(login("https://issuer.test", "subject", None, 40))
                    .await
                    .expect("original binding remains")
            ),
            first,
        );
    }

    #[tokio::test]
    async fn concurrent_first_logins_converge_for_selected_database() {
        let fixture = TestDatabase::new(coral_db::migration_plan())
            .await
            .expect("create authentication test database");
        let directory = UserDirectory::new(fixture.database());

        let (first, second, third) = tokio::join!(
            directory.provision_login(login("https://issuer.test", "subject", Some("A"), 10)),
            directory.provision_login(login("https://issuer.test", "subject", Some("B"), 20)),
            directory.provision_login(login("https://issuer.test", "subject", Some("C"), 30)),
        );

        let first = provisioned(first.expect("first concurrent login"));
        assert_eq!(provisioned(second.expect("second concurrent login")), first);
        assert_eq!(provisioned(third.expect("third concurrent login")), first);
    }

    const fn login<'a>(
        issuer: &'a str,
        subject: &'a str,
        display_name: Option<&'a str>,
        occurred_at_unix_nanos: i64,
    ) -> VerifiedOidcLogin<'a> {
        VerifiedOidcLogin::new(issuer, subject, display_name, occurred_at_unix_nanos)
    }

    fn provisioned(outcome: LoginProvisioning) -> super::UserId {
        match outcome {
            LoginProvisioning::Provisioned(user_id) => user_id,
            LoginProvisioning::IssuerMismatch | LoginProvisioning::Disabled => {
                panic!("expected provisioned login")
            }
        }
    }
}
