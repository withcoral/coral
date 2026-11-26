//! Coral User access-token issuance, credential-to-Principal authentication, and
//! stable User identity provisioning for verified sign-ins.
//!
//! This private domain Module owns the durable mapping from one verified
//! [`UserIdentifier`] to one opaque Coral [`UserId`]. The authorization-server Adapter
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
//!   [`PrincipalAuthentication::local`] serves auth-disabled deployments with the
//!   same interface: a missing credential is the implicit local Owner, except where
//!   the credential must pick the Workspace, and any credential must be an Agent's.
//!
//! # Invariants
//!
//! Every login looks for its User in the store for User Identifiers first. A
//! Subject that the store lacks is then looked for in `users.subject`, among
//! the Users that hold no identifier, and linked: the release before this one
//! writes only that column. A first login mints one random Coral User identity. Concurrent first logins
//! converge on that identity, and later logins preserve it. A subject already
//! bound to another configured issuer is never rebound automatically. Failed
//! provisioning commits no directory or Membership change. Only the explicitly
//! configured bootstrap identifier selects an initial Owner; ordinary login
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
mod user_identifiers;
pub mod user_tokens;

pub use authentication::{AuthenticationError, PrincipalAuthentication, WorkspaceFixedBy};
pub use session::{IssuedAccessToken, SessionTokenError, SessionTokenIssuer};
pub use user_identifiers::{UserIdentifier, UserIdentifierKind};

use std::sync::Arc;

use coral_db::{Database, Transaction, UnixNanos};
use coral_telemetry::safe;
pub use coral_workspace::UserId;
use coral_workspace::{
    AuthenticatedUser, ChangeContext, OrganizationModule, OrganizationRole, TraceId,
};
use user_identifiers::LinkOutcome;

/// External credentials may identify a User, but never the implicit local Owner.
fn external_user_id(value: &str) -> Result<UserId, ()> {
    let id = UserId::parse(value).map_err(|_error| ())?;
    if &id == coral_workspace::local_organization_owner().user_id() {
        return Err(());
    }
    Ok(id)
}

/// Logs why the database failed a statement and returns `refusal`, which
/// reports the failure to the caller. The refusal is all the caller sees and
/// `coral-db` logs nothing, so this record is the only trace of the cause. It
/// names the error's kind and whether the database answered, never the
/// error's text.
fn database_failure<R>(error: &coral_db::Error, refusal: R) -> R {
    if error.is_unanswered() {
        tracing::warn!(
            error.type = safe::kind(error.code()),
            "the database did not answer the authorization server"
        );
    } else {
        tracing::error!(
            error.type = safe::kind(error.code()),
            "the database failed a statement of the authorization server"
        );
    }
    refusal
}

/// One verified sign-in supplied by the authorization-server Adapter. Each kind
/// of identifier has its own constructor, so a Subject always comes with the
/// issuer that verified it.
pub struct VerifiedLogin<'a> {
    identifier: UserIdentifier<'a>,
    /// The verified issuer of an OIDC login. Its User keeps it and the Subject
    /// in `users`, because the release before this one reads them there.
    oidc_issuer: Option<&'a str>,
    display_name: Option<&'a str>,
    profile: coral_workspace::VerifiedUserProfile<'a>,
    profile_observed_at: UnixNanos,
    occurred_at: UnixNanos,
}

impl<'a> VerifiedLogin<'a> {
    /// Builds the persistence input for one successfully verified OIDC login:
    /// the Subject `subject`, verified under `issuer`.
    #[must_use]
    pub const fn oidc(
        issuer: &'a str,
        subject: &'a str,
        display_name: Option<&'a str>,
        occurred_at: UnixNanos,
    ) -> Self {
        Self {
            identifier: UserIdentifier::new(UserIdentifierKind::Subject, subject),
            oidc_issuer: Some(issuer),
            // The other fields are the same for every kind of identifier.
            ..Self::name_id(subject, display_name, occurred_at)
        }
    }

    /// Builds the persistence input for a verified sign-in that names its
    /// person by the value of a `NameId`. A new User of such a sign-in keeps no
    /// issuer and no Subject in `users`, so the release before this one cannot
    /// find it. No sign-in that Coral ships calls this yet.
    #[must_use]
    pub const fn name_id(
        value: &'a str,
        display_name: Option<&'a str>,
        occurred_at: UnixNanos,
    ) -> Self {
        Self {
            identifier: UserIdentifier::new(UserIdentifierKind::NameId, value),
            oidc_issuer: None,
            display_name,
            profile: coral_workspace::VerifiedUserProfile::new(None, None, None, None),
            profile_observed_at: occurred_at,
            occurred_at,
        }
    }

    /// The issuer and the Subject that a new User of this login keeps in
    /// `users`. An OIDC login keeps both, because the release before this one
    /// reads them there. Any other login keeps neither.
    fn kept_columns(&self) -> Option<(&'a str, &'a str)> {
        self.oidc_issuer
            .map(|issuer| (issuer, self.identifier.value()))
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
    pub const fn with_profile_observed_at(mut self, observed_at: UnixNanos) -> Self {
        self.profile_observed_at = observed_at;
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
    bootstrap_owner: Option<(UserIdentifierKind, String)>,
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

    /// Selects the exact verified identifier eligible for initial Owner
    /// provisioning: its kind and its value. The issuer is no part of it, since
    /// an OIDC login is always verified under the configured issuer.
    /// This operator setting is never taken from client metadata or an email address.
    /// It has no effect on an existing Membership's role or disabled state.
    #[must_use]
    pub fn with_bootstrap_owner(mut self, identifier: UserIdentifier<'_>) -> Self {
        self.bootstrap_owner = Some((identifier.kind(), identifier.value().to_owned()));
        self
    }

    /// Creates or refreshes the directory mapping for one verified login.
    ///
    /// # Errors
    ///
    /// Returns a persistence or corrupt-data error. Failure commits no change.
    pub async fn provision_login(
        &self,
        login: VerifiedLogin<'_>,
    ) -> Result<LoginProvisioning, Error> {
        let mut transaction = self.database.begin().await?;
        // Every login holds this row before it reads or writes a User, in this
        // release and in the one before it, so two logins never interleave.
        OrganizationModule::hold_profile_updates_in_transaction(&mut transaction).await?;
        let user_id = match locate_user(&mut transaction, &login).await? {
            Located::Created(user_id) => user_id,
            Located::Existing(user_id) => {
                let stored_issuer = repository::issuer(&mut transaction, &user_id).await?;
                if let (Some(stored), Some(issuer)) = (stored_issuer, login.oidc_issuer)
                    && stored != issuer
                {
                    transaction.rollback().await?;
                    return Ok(LoginProvisioning::IssuerMismatch);
                }
                repository::record_login(&mut transaction, &user_id, &login).await?;
                user_id
            }
        };

        let initial_role = if self
            .bootstrap_owner
            .as_ref()
            .is_some_and(|(kind, value)| UserIdentifier::new(*kind, value) == login.identifier)
        {
            OrganizationRole::Owner
        } else {
            OrganizationRole::Member
        };
        let membership = OrganizationModule::provision_login_in_transaction(
            &mut transaction,
            &ChangeContext::new(
                AuthenticatedUser::organization_member(user_id.clone(), initial_role),
                login.occurred_at,
                TraceId::current_or_new(),
            ),
        )
        .await?;
        OrganizationModule::refresh_profile_in_transaction(
            &mut transaction,
            &user_id,
            login.profile,
            login.profile_observed_at,
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

/// The User of a login, and whether this login made it.
enum Located {
    Existing(UserId),
    Created(UserId),
}

/// Finds the User that holds the login's identifier, or makes it.
///
/// A User that the release before this one made holds its Subject in
/// `users.subject` and no row in the store: such a User is found there, among
/// the Users that hold no identifier, and the store gains its row.
///
/// A first login links the identifier to a new Coral User ID, then `users`
/// gains the User's row. The link inserts with `ON CONFLICT DO NOTHING` and
/// reads again, so a login of the same person that linked first wins.
async fn locate_user(
    transaction: &mut Transaction<'_>,
    login: &VerifiedLogin<'_>,
) -> Result<Located, Error> {
    let identifier = login.identifier;
    if let Some(user_id) = user_identifiers::find(&mut *transaction, identifier).await? {
        return Ok(Located::Existing(user_id));
    }
    if identifier.kind() == UserIdentifierKind::Subject
        && let Some(user_id) =
            repository::find_unlinked_subject(transaction, identifier.value()).await?
    {
        let outcome =
            user_identifiers::link(transaction, identifier, &user_id, login.occurred_at).await?;
        return holder_after_link(outcome, &user_id).map(Located::Existing);
    }
    let candidate = UserId::parse(&uuid::Uuid::new_v4().to_string())?;
    let outcome =
        user_identifiers::link(transaction, identifier, &candidate, login.occurred_at).await?;
    let holder = holder_after_link(outcome, &candidate)?;
    if holder != candidate {
        return Ok(Located::Existing(holder));
    }
    repository::insert_user(transaction, &candidate, login).await?;
    Ok(Located::Created(candidate))
}

/// The User that the store names for an identifier after a link of it to
/// `user_id`. A User that held no identifier cannot hold another of the kind.
fn holder_after_link(outcome: LinkOutcome, user_id: &UserId) -> Result<UserId, Error> {
    match outcome {
        LinkOutcome::Linked | LinkOutcome::AlreadyLinked => Ok(user_id.clone()),
        LinkOutcome::HeldBy(holder) => Ok(holder),
        LinkOutcome::UserHoldsAnother => Err(Error::CorruptData(
            "the store refused a link for a User that held no identifier of its kind".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{LoginProvisioning, UserDirectory, UserId, VerifiedLogin};
    use crate::user_identifiers::tests::{
        ISSUER, assert_holders, insert_user_as_the_release_before, name_id, subject,
    };
    use coral_test_support::TestDatabase;
    use sea_query::{Alias, Expr, ExprTrait as _, Query};

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
            coral_db::UnixNanos::new(11),
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
                            .with_profile_observed_at(coral_db::UnixNanos::new(5))
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
        let directory = UserDirectory::new(fixture.database()).with_bootstrap_owner(
            super::UserIdentifier::new(super::UserIdentifierKind::Subject, "owner"),
        );
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
        let context = ChangeContext::new(
            owner.clone(),
            coral_db::UnixNanos::new(10),
            TraceId::current_or_new(),
        );
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
        let directory = UserDirectory::new(fixture.database()).with_bootstrap_owner(
            super::UserIdentifier::new(super::UserIdentifierKind::Subject, "owner"),
        );
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
            coral_db::UnixNanos::new(2),
            coral_workspace::TraceId::parse(&"a".repeat(32)).unwrap(),
        );
        organization
            .set_enabled(&change, &member_id, &coral_workspace::IfMatch::any(), false)
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

    /// Concurrent first logins of one person make one User, whose identifier
    /// the store holds, for each kind. Both kinds use one value on purpose:
    /// the `NameId` case finds no User of the Subject case.
    #[tokio::test]
    async fn concurrent_first_logins_converge_for_selected_database() {
        type Login = fn(Option<&'static str>, i64) -> VerifiedLogin<'static>;
        let fixture = TestDatabase::new(coral_db::migration_plan())
            .await
            .expect("create authentication test database");
        let directory = UserDirectory::new(fixture.database());
        let cases: [(&str, super::UserIdentifier<'static>, Login); 2] = [
            ("an OIDC Subject", subject("subject"), |name, at| {
                login(ISSUER, "subject", name, at)
            }),
            ("a NameID", name_id("subject"), |name, at| {
                name_id_login("subject", name, at)
            }),
        ];

        for (case, identifier, login) in cases {
            let (first, second, third) = tokio::join!(
                directory.provision_login(login(Some("A"), 10)),
                directory.provision_login(login(Some("B"), 20)),
                directory.provision_login(login(Some("C"), 30)),
            );

            let first = provisioned(first.expect("first concurrent login"));
            assert_eq!(
                provisioned(second.expect("second concurrent login")),
                first,
                "{case}"
            );
            assert_eq!(
                provisioned(third.expect("third concurrent login")),
                first,
                "{case}"
            );
            assert_holders(&fixture, &[(identifier, Some(&first))]).await;
        }
    }

    /// A User that the release before this one made during the upgrade holds
    /// its Subject in `users.subject` and no row in the store. Its sign-in
    /// finds it there, and the store gains the row. A sign-in under another
    /// issuer is refused and links nothing.
    #[tokio::test]
    async fn a_user_with_a_subject_in_its_column_and_no_row_is_found_and_linked() {
        let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
        let directory = UserDirectory::new(fixture.database());
        let dana = insert_user_as_the_release_before(&fixture, "dana").await;

        assert_eq!(
            directory
                .provision_login(login("https://other.test", "dana", None, 1))
                .await
                .unwrap(),
            LoginProvisioning::IssuerMismatch
        );
        assert_holders(&fixture, &[(subject("dana"), None)]).await;
        for at in [2, 3] {
            let user = directory.provision_login(login(ISSUER, "dana", None, at));
            assert_eq!(provisioned(user.await.unwrap()), dana);
            assert_holders(&fixture, &[(subject("dana"), Some(&dana))]).await;
        }
    }

    /// A sign-in by another kind than a Subject makes a User whose issuer and
    /// Subject in `users` are empty. The store finds it again, and a Subject of
    /// the same value names another person, whose row keeps both.
    #[tokio::test]
    async fn a_user_whose_two_columns_are_empty_is_found_through_the_store() {
        let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
        let directory = UserDirectory::new(fixture.database());
        let gil = provisioned(
            directory
                .provision_login(name_id_login("gil", None, 1))
                .await
                .unwrap(),
        );
        assert_eq!(columns(&fixture, &gil).await, (None, None));
        let again = directory.provision_login(name_id_login("gil", None, 2));
        assert_eq!(provisioned(again.await.unwrap()), gil);

        let subject_gil = directory.provision_login(login(ISSUER, "gil", None, 3));
        let subject_gil = provisioned(subject_gil.await.unwrap());
        assert_ne!(subject_gil, gil);
        assert_eq!(
            columns(&fixture, &subject_gil).await,
            (Some(ISSUER.to_owned()), Some("gil".to_owned()))
        );
        assert_holders(
            &fixture,
            &[
                (name_id("gil"), Some(&gil)),
                (subject("gil"), Some(&subject_gil)),
            ],
        )
        .await;
    }

    /// A disabled User whom the release before this one made is refused at
    /// each sign-in. The refused sign-in still links the Subject, as it records
    /// the login: the link names the person and grants nothing.
    #[tokio::test]
    async fn a_disabled_user_of_the_release_before_stays_refused() {
        use coral_test_support::{change, provision_user};
        use coral_workspace::{OrganizationModule, OrganizationRole};
        let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
        let directory =
            UserDirectory::new(fixture.database()).with_bootstrap_owner(subject("owner"));
        let owner = provisioned(
            directory
                .provision_login(login(ISSUER, "owner", None, 1))
                .await
                .unwrap(),
        );
        let erin = insert_user_as_the_release_before(&fixture, "erin").await;
        provision_user(fixture.database(), erin.as_str(), OrganizationRole::Member).await;
        let organization = OrganizationModule::new(fixture.database());
        let owner = organization.resolve_user_authority(&owner).await.unwrap();
        organization
            .set_enabled(
                &change(owner, 2, 'a'),
                &erin,
                &coral_workspace::IfMatch::any(),
                false,
            )
            .await
            .unwrap();

        assert_holders(&fixture, &[(subject("erin"), None)]).await;
        for at in [3, 4] {
            assert_eq!(
                directory
                    .provision_login(login(ISSUER, "erin", None, at))
                    .await
                    .unwrap(),
                LoginProvisioning::Disabled
            );
            assert_holders(&fixture, &[(subject("erin"), Some(&erin))]).await;
        }
    }

    /// The first Owner is named by a kind and a value. An OIDC login is always
    /// verified under the configured issuer, so for every OIDC login this gives
    /// the answer that the match on the issuer and the Subject gave.
    #[tokio::test]
    async fn the_first_owner_is_the_login_with_the_configured_kind_and_value() {
        use coral_workspace::OrganizationRole::{Member, Owner};
        let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
        let cases = [
            (
                "the configured Subject",
                subject("o1"),
                login(ISSUER, "o1", None, 1),
                Owner,
            ),
            (
                "another Subject",
                subject("o2"),
                login(ISSUER, "m2", None, 1),
                Member,
            ),
            (
                "the Subject in another case",
                subject("o3"),
                login(ISSUER, "O3", None, 1),
                Member,
            ),
            (
                "the Subject's value as a NameID",
                subject("o4"),
                name_id_login("o4", None, 1),
                Member,
            ),
            (
                "the configured NameID",
                name_id("o5"),
                name_id_login("o5", None, 1),
                Owner,
            ),
            (
                "the NameID's value as a Subject",
                name_id("o6"),
                login(ISSUER, "o6", None, 1),
                Member,
            ),
        ];
        let organization = coral_workspace::OrganizationModule::new(fixture.database());
        for (case, owner, login, expected) in cases {
            let directory = UserDirectory::new(fixture.database()).with_bootstrap_owner(owner);
            let user = provisioned(directory.provision_login(login).await.unwrap());
            let role = organization
                .resolve_user_authority(&user)
                .await
                .unwrap()
                .organization_role();
            assert_eq!(role, Some(expected), "{case}");
        }
    }

    /// The issuer and the Subject that the User's `users` row keeps.
    async fn columns(fixture: &TestDatabase, user_id: &UserId) -> (Option<String>, Option<String>) {
        fixture
            .database()
            .fetch_optional(
                Query::select()
                    .columns([Alias::new("issuer"), Alias::new("subject")])
                    .from(Alias::new("users"))
                    .and_where(Expr::col(Alias::new("user_id")).eq(user_id.as_str()))
                    .to_owned(),
            )
            .await
            .unwrap()
            .expect("the User has a users row")
    }

    const fn name_id_login<'a>(
        value: &'a str,
        display_name: Option<&'a str>,
        occurred_at_unix_nanos: i64,
    ) -> VerifiedLogin<'a> {
        VerifiedLogin::name_id(
            value,
            display_name,
            coral_db::UnixNanos::new(occurred_at_unix_nanos),
        )
    }

    const fn login<'a>(
        issuer: &'a str,
        subject: &'a str,
        display_name: Option<&'a str>,
        occurred_at_unix_nanos: i64,
    ) -> VerifiedLogin<'a> {
        VerifiedLogin::oidc(
            issuer,
            subject,
            display_name,
            coral_db::UnixNanos::new(occurred_at_unix_nanos),
        )
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
