mod updates;

use super::*;
use coral_provider::access::{CredentialRoute, ProviderNamespace};
use coral_provider::account::{AccountModule, AccountScope, AuthorizationState};
use coral_provider::{
    ManagementRequestId, OpenApiDocument, ProviderId, ProviderModule,
    WorkspaceProviderPublicationRequest,
};

pub(super) async fn prepare_accounts(
    database: Arc<Database>,
    workspaces: WorkspaceModule,
    owner: &AuthenticatedUser,
    workspace: &WorkspaceId,
) -> (ProviderId, AccountModule, tempfile::TempDir) {
    let providers = ProviderModule::with_running_catalog(
        coral_outbound::DestinationPolicy::default()
            .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
        database,
        workspaces,
    );
    let provider = providers
        .publish_openapi_fixture(
            &change(owner),
            workspace,
            OpenApiDocument::parse(BEARER_OPENAPI).unwrap(),
            "private",
            CredentialRoute::Caller,
        )
        .await
        .unwrap()
        .access()
        .id()
        .clone();
    let key = tempfile::tempdir().unwrap();
    let accounts = AccountModule::new(
        providers,
        coral_credentials::CredentialVault::open(&key.path().join("key")).unwrap(),
    );
    (provider, accounts, key)
}

pub(super) async fn credential_evidence(
    database: &Database,
    workspace: &WorkspaceId,
    user: &AuthenticatedUser,
) -> coral_provider::account::CredentialEvidence {
    let mut tx = database.begin().await.unwrap();
    let target = coral_provider::resolve_owner_tool_in_transaction(
        &mut tx,
        workspace,
        "coral.providers.private.value",
    )
    .await
    .unwrap()
    .unwrap();
    let evidence = coral_provider::account::select_in_transaction(
        &mut tx,
        &target,
        AuthenticatedPrincipal::user(user.clone()).principal(),
    )
    .await
    .unwrap()
    .unwrap();
    tx.commit().await.unwrap();
    evidence
}

pub(super) struct ConnectedScope {
    _key: tempfile::TempDir,
    pub(super) accounts: AccountModule,
    pub(super) provider: ProviderId,
    workspace: WorkspaceId,
    evidence: coral_provider::account::CredentialEvidence,
    /// The connected personal Account.
    pub(super) account: coral_provider::account::ProviderAccountId,
    /// A pending reconnect of that Account.
    pub(super) pending: coral_provider::account::AuthorizationId,
    events: Vec<coral_provider::AdministrativeLifecycleEvent>,
}
impl ConnectedScope {
    pub(super) async fn new(
        database: Arc<Database>,
        workspaces: WorkspaceModule,
        owner: &AuthenticatedUser,
        user: &AuthenticatedUser,
        workspace: &WorkspaceId,
    ) -> Self {
        let (provider, accounts, key) =
            prepare_accounts(database.clone(), workspaces, owner, workspace).await;
        let context = change(user);
        let initial = accounts
            .start_user_authorization(&context, workspace, &provider, request(), None)
            .await
            .unwrap();
        let connected = accounts
            .submit_secret(
                &context,
                workspace,
                initial.id,
                coral_credentials::CredentialValues::token(
                    coral_credentials::Secret::new("fixture-secret".into()).unwrap(),
                ),
            )
            .await
            .unwrap();
        let account = connected.account_id.unwrap();
        let pending = accounts
            .start_reconnect(&context, workspace, &provider, account, request(), None)
            .await
            .unwrap();
        let evidence = credential_evidence(&database, workspace, user).await;
        let events = accounts
            .list_user_lifecycle_events(&context, workspace)
            .await
            .unwrap();
        Self {
            _key: key,
            accounts,
            provider,
            workspace: workspace.clone(),
            evidence,
            account,
            pending: pending.id,
            events,
        }
    }
    pub(super) async fn assert_unchanged(&self, user: &AuthenticatedUser) {
        assert_eq!(
            self.accounts
                .credentials(&self.workspace, &self.provider, &self.evidence, 100)
                .await
                .unwrap()
                .get(coral_credentials::CredentialValues::TOKEN)
                .unwrap()
                .expose(),
            "fixture-secret"
        );
        assert_eq!(
            self.accounts
                .get_authorization(&change(user), &self.workspace, self.pending)
                .await
                .unwrap()
                .state,
            AuthorizationState::Pending
        );
        assert_eq!(
            self.accounts
                .list_user_lifecycle_events(&change(user), &self.workspace)
                .await
                .unwrap(),
            self.events
        );
    }
    pub(super) async fn assert_retired(&self, user: &AuthenticatedUser) {
        self.accounts
            .credentials(&self.workspace, &self.provider, &self.evidence, 100)
            .await
            .unwrap_err();
        assert!(
            self.accounts
                .get_account(
                    &change(user),
                    &self.workspace,
                    &self.provider,
                    &AccountScope::User
                )
                .await
                .unwrap()
                .account
                .is_none()
        );
        assert_eq!(
            self.accounts
                .get_authorization(&change(user), &self.workspace, self.pending)
                .await
                .unwrap()
                .state,
            AuthorizationState::Canceled
        );
    }
}

struct AccountFixture {
    db: coral_test_support::TestDatabase,
    _key: tempfile::TempDir,
    workspaces: WorkspaceModule,
    organization: coral_workspace::OrganizationModule,
    lifecycle: AccessLifecycleModule,
    accounts: AccountModule,
    owner: AuthenticatedUser,
    member: AuthenticatedUser,
    workspace: coral_workspace::Workspace,
    provider: ProviderId,
}
pub(super) fn request() -> ManagementRequestId {
    ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap()
}
impl AccountFixture {
    async fn new() -> Self {
        let db = fixture().await;
        let database = db.database();
        let workspaces = WorkspaceModule::new(database.clone());
        let owner = provision_user(database.clone(), "user:owner", OrganizationRole::Owner).await;
        let member =
            provision_user(database.clone(), "user:member", OrganizationRole::Member).await;
        let workspace = workspaces
            .create(
                &change(&member),
                WorkspaceDisplayName::parse("Accounts").unwrap(),
            )
            .await
            .unwrap();
        join_as_owner(&workspaces, &owner, workspace.id()).await;
        let (provider, accounts, key) =
            prepare_accounts(database.clone(), workspaces.clone(), &owner, workspace.id()).await;
        Self {
            db,
            _key: key,
            lifecycle: AccessLifecycleModule::new(database.clone(), workspaces.clone()),
            organization: coral_workspace::OrganizationModule::new(database),
            workspaces,
            accounts,
            owner,
            member,
            workspace,
            provider,
        }
    }
    async fn connect(&self, user: &AuthenticatedUser) -> coral_provider::account::Authorization {
        let authorization = self
            .accounts
            .start_user_authorization(
                &change(user),
                self.workspace.id(),
                &self.provider,
                request(),
                None,
            )
            .await
            .unwrap();
        self.accounts
            .submit_secret(
                &change(user),
                self.workspace.id(),
                authorization.id,
                coral_credentials::CredentialValues::token(
                    coral_credentials::Secret::new("fixture-secret".into()).unwrap(),
                ),
            )
            .await
            .unwrap()
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one shared Account journey verifies provisioner departure and stale replacement after rejoin"
)]
async fn shared_account_survives_its_provisioner_but_pending_replacement_is_canceled() {
    let f = AccountFixture::new().await;
    let providers = ProviderModule::with_running_catalog(
        coral_outbound::DestinationPolicy::default()
            .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
        f.db.database(),
        f.workspaces.clone(),
    );
    let context = change(&f.member);
    let setup = providers
        .prepare_setup(
            &context,
            f.workspace.id(),
            coral_provider::ProviderSetupSource::Current,
            Some(&f.provider),
        )
        .await
        .unwrap();
    let review = providers
        .review_setup_configuration(&f.member, f.workspace.id(), setup.id())
        .await
        .unwrap();
    let review = providers
        .confirm_setup_configuration(
            &context,
            f.workspace.id(),
            setup.id(),
            review.revision,
            review.confirmed.unwrap(),
        )
        .await
        .unwrap();
    let grant = f
        .accounts
        .authorize_setup(
            &context,
            f.workspace.id(),
            setup.id(),
            review.revision,
            coral_credentials::CredentialValues::token(
                coral_credentials::Secret::new("shared-secret".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    providers
        .regenerate_provider(
            &context,
            coral_provider::ProviderRegenerationRequest::new(
                request(),
                f.workspace.id().clone(),
                f.provider.clone(),
                setup.id().clone(),
                review.revision,
            )
            .with_account_setting(CredentialRoute::Fixed)
            .with_account_grant(grant.provisional_grant_id.unwrap())
            .with_incompatible_configuration_confirmation(),
        )
        .await
        .unwrap();
    let before = f
        .accounts
        .get_account(
            &change(&f.owner),
            f.workspace.id(),
            &f.provider,
            &AccountScope::Shared,
        )
        .await
        .unwrap();
    let pending = f
        .accounts
        .start_reconnect(
            &context,
            f.workspace.id(),
            &f.provider,
            before.account.as_ref().unwrap().id,
            request(),
            None,
        )
        .await
        .unwrap();
    f.lifecycle
        .remove_organization_member(&change(&f.owner), f.member.user_id(), 1, None)
        .await
        .unwrap();
    assert_eq!(
        f.accounts
            .get_account(
                &change(&f.owner),
                f.workspace.id(),
                &f.provider,
                &AccountScope::Shared
            )
            .await
            .unwrap(),
        before
    );
    f.organization.provision_login(&context).await.unwrap();
    f.workspaces
        .set_membership(
            &change(&f.owner),
            f.workspace.id(),
            &f.member,
            coral_workspace::WorkspaceRole::Owner,
        )
        .await
        .unwrap();
    assert_eq!(
        f.accounts
            .get_authorization(&context, f.workspace.id(), pending.id)
            .await
            .unwrap()
            .state,
        AuthorizationState::Canceled
    );
    assert_eq!(
        f.accounts
            .submit_secret(
                &context,
                f.workspace.id(),
                pending.id,
                coral_credentials::CredentialValues::token(
                    coral_credentials::Secret::new("late-shared-secret".into()).unwrap()
                )
            )
            .await
            .unwrap()
            .state,
        AuthorizationState::Canceled
    );
    assert_eq!(
        f.accounts
            .get_account(
                &change(&f.owner),
                f.workspace.id(),
                &f.provider,
                &AccountScope::Shared
            )
            .await
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn initial_connection_without_credentials_is_canceled_and_cannot_fill_a_rejoined_slot() {
    let f = AccountFixture::new().await;
    let context = change(&f.member);
    let request_id = request();
    let pending = f
        .accounts
        .start_user_authorization(
            &context,
            f.workspace.id(),
            &f.provider,
            request_id.clone(),
            None,
        )
        .await
        .unwrap();
    let old = f
        .accounts
        .get_account(&context, f.workspace.id(), &f.provider, &AccountScope::User)
        .await
        .unwrap()
        .account
        .unwrap();
    assert_eq!(old.connected_at_unix_nanos, None);
    f.lifecycle
        .remove_workspace_member(&context, f.workspace.id(), f.member.user_id())
        .await
        .unwrap();
    f.workspaces
        .set_membership(
            &change(&f.owner),
            f.workspace.id(),
            &f.member,
            coral_workspace::WorkspaceRole::Member,
        )
        .await
        .unwrap();
    assert!(
        f.accounts
            .get_account(&context, f.workspace.id(), &f.provider, &AccountScope::User)
            .await
            .unwrap()
            .account
            .is_none()
    );
    let replay = f
        .accounts
        .start_user_authorization(&context, f.workspace.id(), &f.provider, request_id, None)
        .await
        .unwrap();
    assert_eq!(replay.id, pending.id);
    assert_eq!(replay.state, AuthorizationState::Canceled);
    let fresh = f.connect(&f.member).await;
    assert_ne!(fresh.account_id, Some(old.id));
    assert_eq!(
        f.accounts
            .submit_secret(
                &context,
                f.workspace.id(),
                pending.id,
                coral_credentials::CredentialValues::token(
                    coral_credentials::Secret::new("late-secret".into()).unwrap()
                )
            )
            .await
            .unwrap()
            .state,
        AuthorizationState::Canceled
    );
    assert_eq!(
        f.accounts
            .get_account(&context, f.workspace.id(), &f.provider, &AccountScope::User)
            .await
            .unwrap()
            .account
            .unwrap()
            .id,
        fresh.account_id.unwrap()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn organization_offboarding_serializes_concurrent_save_grant_and_initial_connect() {
    let mut f = AccountFixture::new().await;
    let functions = SavedFunctionModule::new(
        f.db.database(),
        f.workspaces.clone(),
        coral_functions::IsolateLimits::default(),
    )
    .unwrap();
    let principal = AuthenticatedPrincipal::user(f.member.clone());
    save_answer(&functions, &principal, f.workspace.id()).await;
    let gate = Arc::new(RemovalGate::default());
    f.lifecycle.pause_after_prepare = Some(gate.clone());
    let removal = {
        let lifecycle = f.lifecycle.clone();
        let context = change(&f.owner);
        let user = f.member.user_id().clone();
        tokio::spawn(async move {
            lifecycle
                .remove_organization_member(&context, &user, 1, None)
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.prepared.notified())
        .await
        .unwrap();
    let context = change(&f.member);
    let administrator = change(&f.owner);
    let source = SavedFunctionSave::new(
        SavedFunctionPath::parse("late").unwrap(),
        SavedFunctionDescription::parse("Late save").unwrap(),
        SavedFunctionSource::parse("export default () => 7;").unwrap(),
    );
    let authoring =
        SavedFunctionChangeContext::new(principal.clone(), 101, context.trace_id().clone());
    // Poll every competing command while removal retains its preparation holds.
    // The release is the final branch, so no sleeps or scheduler guesses order it.
    let (save, grant, connect, ()) = tokio::join!(
        biased;
        functions.save(&authoring, f.workspace.id(), source),
        f.workspaces.set_membership(&administrator, f.workspace.id(), &f.member, coral_workspace::WorkspaceRole::Member),
        f.accounts.start_user_authorization(&context, f.workspace.id(), &f.provider, request(), None),
        async { gate.proceed.notify_one(); },
    );
    removal.await.unwrap().unwrap();
    save.unwrap_err();
    grant.unwrap_err();
    connect.unwrap_err();
    f.organization.provision_login(&context).await.unwrap();
    f.workspaces
        .set_membership(
            &administrator,
            f.workspace.id(),
            &f.member,
            coral_workspace::WorkspaceRole::Member,
        )
        .await
        .unwrap();
    for path in ["answer", "late"] {
        assert!(matches!(
            functions
                .get(
                    &principal,
                    f.workspace.id(),
                    &SavedFunctionPath::parse(path).unwrap()
                )
                .await,
            Err(coral_functions::Error::SavedFunctionNotFound)
        ));
    }
    assert!(
        f.accounts
            .get_account(&context, f.workspace.id(), &f.provider, &AccountScope::User)
            .await
            .unwrap()
            .account
            .is_none()
    );
}

/// Membership removal clears an Organization Owner's credentials like anyone else's.
#[tokio::test]
async fn membership_removal_clears_an_organization_owners_account() {
    let f = AccountFixture::new().await;
    f.connect(&f.owner).await.account_id.unwrap();
    let evidence = credential_evidence(&f.db.database(), f.workspace.id(), &f.owner).await;
    f.accounts
        .credentials(f.workspace.id(), &f.provider, &evidence, 100)
        .await
        .unwrap();
    f.lifecycle
        .remove_workspace_member(&change(&f.owner), f.workspace.id(), f.owner.user_id())
        .await
        .unwrap();
    // Organization Owner authority alone reaches nothing inside the Workspace.
    assert!(matches!(
        f.accounts
            .get_account(
                &change(&f.owner),
                f.workspace.id(),
                &f.provider,
                &AccountScope::User
            )
            .await,
        Err(coral_provider::Error::Workspace(
            coral_workspace::Error::WorkspaceNotFound
        ))
    ));
    f.accounts
        .credentials(f.workspace.id(), &f.provider, &evidence, 100)
        .await
        .unwrap_err();
    // A new Membership starts with no Account.
    join_as_owner(&f.workspaces, &f.owner, f.workspace.id()).await;
    assert!(
        f.accounts
            .get_account(
                &change(&f.owner),
                f.workspace.id(),
                &f.provider,
                &AccountScope::User
            )
            .await
            .unwrap()
            .account
            .is_none()
    );
}

/// Only Membership removal ends access: disablement and archive keep current Accounts.
#[tokio::test]
async fn disablement_and_archive_preserve_current_accounts() {
    let f = AccountFixture::new().await;
    let member_account = f.connect(&f.member).await.account_id.unwrap();
    f.organization
        .set_enabled(&change(&f.owner), f.member.user_id(), 1, false)
        .await
        .unwrap();
    f.accounts
        .get_account(
            &change(&f.member),
            f.workspace.id(),
            &f.provider,
            &AccountScope::User,
        )
        .await
        .unwrap_err();
    f.organization
        .set_enabled(&change(&f.owner), f.member.user_id(), 2, true)
        .await
        .unwrap();
    assert_eq!(
        f.accounts
            .get_account(
                &change(&f.member),
                f.workspace.id(),
                &f.provider,
                &AccountScope::User
            )
            .await
            .unwrap()
            .account
            .unwrap()
            .id,
        member_account
    );
    f.workspaces
        .archive(&change(&f.owner), f.workspace.id())
        .await
        .unwrap();
    f.workspaces
        .check_run_admission(
            &AuthenticatedPrincipal::user(f.member.clone()),
            f.workspace.id(),
            100,
        )
        .await
        .unwrap_err();
    f.workspaces
        .restore(&change(&f.owner), f.workspace.id())
        .await
        .unwrap();
    assert_eq!(
        f.accounts
            .get_account(
                &change(&f.member),
                f.workspace.id(),
                &f.provider,
                &AccountScope::User
            )
            .await
            .unwrap()
            .account
            .unwrap()
            .id,
        member_account
    );
}
