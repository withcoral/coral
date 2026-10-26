//! Workspace Provider publication, lifecycle and request replay at the domain interface.
#![allow(
    unused_crate_dependencies,
    reason = "integration crate uses only dependencies at the tested domain boundary"
)]
use coral_provider::{
    CredentialRoute, InvocationOperationVerification, LifecycleAction, LifecycleOutcome,
    ManagementRequestId, OpenApiDocument, ProviderCallRoot, ProviderDisplayName, ProviderModule,
    ProviderPublicationOutcome, ProviderRegenerationRequest, ProviderState,
    WorkspaceProviderPublicationRequest,
};
use coral_telemetry::TraceId;
use coral_test_support::{TestDatabase, provision_user};
use coral_workspace::{
    AuthenticatedUser, ChangeContext, OrganizationRole, WorkspaceDisplayName, WorkspaceId,
    WorkspaceModule,
};

const SOURCE: &str = r#"{"openapi":"3.1.0","info":{"title":"Weather","version":"1"},"servers":[{"url":"https://weather.example"}],"security":[],"paths":{"/forecast":{"get":{"operationId":"forecast","responses":{"200":{"description":"OK"}}}}}}"#;

fn context(actor: &AuthenticatedUser) -> ChangeContext {
    ChangeContext::new(
        actor.clone(),
        10,
        TraceId::parse(&"a".repeat(32)).expect("valid trace fixture"),
    )
}

async fn harness() -> (
    TestDatabase,
    WorkspaceModule,
    ProviderModule,
    AuthenticatedUser,
) {
    let db = TestDatabase::new(coral_db::migration_plan())
        .await
        .expect("create domain test database");
    let workspaces = WorkspaceModule::new(db.database());
    let providers = ProviderModule::with_running_catalog(
        coral_outbound::DestinationPolicy::default()
            .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
        db.database(),
        workspaces.clone(),
    );
    let actor = provision_user(
        db.database(),
        "user:workspace-provider",
        OrganizationRole::Owner,
    )
    .await;
    (db, workspaces, providers, actor)
}

async fn setup(
    providers: &ProviderModule,
    actor: &AuthenticatedUser,
    workspace: &WorkspaceId,
) -> coral_provider::ProviderSetup {
    setup_from(providers, actor, workspace, SOURCE).await
}

async fn setup_from(
    providers: &ProviderModule,
    actor: &AuthenticatedUser,
    workspace: &WorkspaceId,
    source: &str,
) -> coral_provider::ProviderSetup {
    let setup = providers
        .create_openapi_setup(
            &context(actor),
            workspace,
            OpenApiDocument::parse(source).expect("valid OpenAPI fixture"),
        )
        .await
        .expect("prepare Workspace Setup");
    let review = providers
        .review_setup_configuration(actor, workspace, setup.id())
        .await
        .expect("review fixture configuration");
    let values = review
        .document
        .expect("OpenAPI configuration review")
        .suggested;
    let confirmed = providers
        .confirm_setup_configuration(
            &context(actor),
            workspace,
            setup.id(),
            review.revision,
            values,
        )
        .await
        .expect("confirm fixture configuration");
    assert!(confirmed.errors.is_empty());
    providers
        .get_setup(actor, workspace, setup.id())
        .await
        .expect("reload configured Setup")
}

/// The `(action, outcome)` of each Lifecycle Event a change added.
fn added_events(
    before: &[coral_provider::AdministrativeLifecycleEvent],
    after: &[coral_provider::AdministrativeLifecycleEvent],
) -> Vec<(LifecycleAction, LifecycleOutcome)> {
    after
        .iter()
        .filter(|event| !before.contains(event))
        .map(|event| (event.action(), event.outcome()))
        .collect()
}

/// The Operation catalogs discovery would search in `workspace`.
async fn searchable_providers(
    database: &coral_db::Database,
    workspace: &WorkspaceId,
) -> Vec<ProviderCallRoot> {
    let mut tx = database.begin().await.expect("begin discovery read");
    let catalogs =
        coral_provider::provider_search_catalogs_in_transaction(&mut tx, workspace, None)
            .await
            .expect("read searchable catalogs");
    tx.rollback().await.expect("finish discovery read");
    catalogs
        .iter()
        .map(|catalog| catalog.access().call_root().clone())
        .collect()
}

fn request(
    workspace: &WorkspaceId,
    setup: &coral_provider::ProviderSetup,
    namespace: &str,
    id: &str,
) -> WorkspaceProviderPublicationRequest {
    WorkspaceProviderPublicationRequest::new(
        ManagementRequestId::parse(id).expect("valid publication request fixture"),
        workspace.clone(),
        setup.id().clone(),
        coral_provider::ProviderNamespace::parse(namespace).expect("valid namespace fixture"),
        CredentialRoute::None,
    )
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one domain journey preserves authority evidence across dependent lifecycle transitions"
)]
async fn one_provider_owns_namespace_lifecycle_and_retained_generation() {
    let (db, workspaces, providers, actor) = harness().await;
    let database = db.database();
    let workspace = workspaces
        .create(
            &context(&actor),
            WorkspaceDisplayName::parse("Weather").unwrap(),
        )
        .await
        .unwrap();
    let setup = setup(&providers, &actor, workspace.id()).await;
    let request = request(
        workspace.id(),
        &setup,
        "weather",
        "81000000-0000-4000-8000-000000000001",
    );
    let publication = providers
        .publish_workspace_provider(&context(&actor), request.clone())
        .await
        .unwrap();
    assert_eq!(publication.outcome(), ProviderPublicationOutcome::Succeeded);
    let provider = publication.published_provider().unwrap();
    assert_eq!(provider.call_root().as_str(), "coral.providers.weather");
    assert_eq!(provider.account_setting_version(), 1);
    assert_eq!(provider.access().id(), provider.id());
    assert_eq!(
        providers
            .inspect_providers(&actor, workspace.id())
            .await
            .unwrap(),
        std::slice::from_ref(provider)
    );

    let mut tx = database.begin().await.unwrap();
    let retained = coral_provider::resolve_owner_operation_in_transaction(
        &mut tx,
        workspace.id(),
        &format!("{}.forecast", provider.call_root().as_str()),
    )
    .await
    .unwrap()
    .unwrap();
    tx.commit().await.unwrap();

    let renamed = providers
        .rename_provider(
            &context(&actor).with_etag(&provider.etag()).unwrap(),
            workspace.id(),
            provider.id(),
            ProviderDisplayName::parse("Forecast").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(renamed.id(), provider.id());
    assert_eq!(renamed.call_root(), provider.call_root());
    assert_eq!(renamed.account_setting_version(), 1);
    assert!(matches!(
        providers
            .rename_provider(
                &context(&actor).with_etag(&provider.etag()).unwrap(),
                workspace.id(),
                provider.id(),
                ProviderDisplayName::parse("Stale").unwrap()
            )
            .await,
        Err(coral_provider::Error::EtagMismatch)
    ));

    let update_setup = self::setup(&providers, &actor, workspace.id()).await;
    let revision = providers
        .review_setup_configuration(&actor, workspace.id(), update_setup.id())
        .await
        .unwrap()
        .revision;
    providers
        .regenerate_provider(
            &context(&actor).with_etag(&renamed.etag()).unwrap(),
            ProviderRegenerationRequest::new(
                ManagementRequestId::parse("81000000-0000-4000-8000-000000000003").unwrap(),
                workspace.id().clone(),
                provider.id().clone(),
                update_setup.id().clone(),
                revision,
            ),
        )
        .await
        .unwrap();
    let updated = providers
        .inspect_provider(&actor, workspace.id(), provider.id())
        .await
        .unwrap();
    assert_eq!(updated.call_root(), provider.call_root());
    assert_eq!(updated.account_setting_version(), 1);
    let mut tx = database.begin().await.unwrap();
    assert_eq!(
        coral_provider::verify_invocation_operation_in_transaction(
            &mut tx,
            workspace.id(),
            &retained,
            coral_db::RowLock::Shared,
        )
        .await
        .unwrap(),
        coral_provider::InvocationOperationVerification::Current
    );
    tx.rollback().await.unwrap();
    let conflict_setup = self::setup(&providers, &actor, workspace.id()).await;
    let conflict = providers
        .publish_workspace_provider(
            &context(&actor),
            self::request(
                workspace.id(),
                &conflict_setup,
                "weather",
                "81000000-0000-4000-8000-000000000004",
            ),
        )
        .await
        .unwrap();
    assert_eq!(conflict.outcome(), ProviderPublicationOutcome::Failed);
    assert_eq!(
        providers
            .inspect_providers(&actor, workspace.id())
            .await
            .unwrap(),
        std::slice::from_ref(&updated)
    );
    let events = providers
        .list_lifecycle_events(&actor, workspace.id())
        .await
        .unwrap();
    let disabled = providers
        .disable_provider(
            &context(&actor).with_etag(&updated.etag()).unwrap(),
            workspace.id(),
            provider.id(),
        )
        .await
        .unwrap();
    assert_eq!(disabled.state(), ProviderState::Disabled);
    assert_eq!(
        providers
            .inspect_providers(&actor, workspace.id())
            .await
            .unwrap(),
        std::slice::from_ref(&disabled),
        "disablement keeps the Provider identity"
    );
    assert_eq!(
        providers
            .inspect_operation(
                &actor,
                workspace.id(),
                provider.id(),
                &coral_catalog::OperationKey::parse("forecast").unwrap(),
            )
            .await
            .unwrap()
            .key()
            .as_str(),
        "forecast",
        "disablement does not hide administrative catalog inspection"
    );
    assert!(
        searchable_providers(&database, workspace.id())
            .await
            .is_empty()
    );
    let after_disable = providers
        .list_lifecycle_events(&actor, workspace.id())
        .await
        .unwrap();
    assert_eq!(
        added_events(&events, &after_disable),
        [(
            LifecycleAction::ProviderDisabled,
            LifecycleOutcome::Accepted
        )]
    );
    let mut tx = database.begin().await.unwrap();
    assert!(
        coral_provider::resolve_owner_operation_in_transaction(
            &mut tx,
            workspace.id(),
            &format!("{}.forecast", provider.call_root().as_str()),
        )
        .await
        .unwrap()
        .is_none()
    );
    assert_eq!(
        coral_provider::verify_invocation_operation_in_transaction(
            &mut tx,
            workspace.id(),
            &retained,
            coral_db::RowLock::Shared,
        )
        .await
        .unwrap(),
        InvocationOperationVerification::AuthorityLost,
        "disablement blocks retained work"
    );
    tx.rollback().await.unwrap();
    let restored = providers
        .restore_provider(
            &context(&actor).with_etag(&disabled.etag()).unwrap(),
            workspace.id(),
            provider.id(),
        )
        .await
        .unwrap();
    assert_eq!(restored.state(), ProviderState::Active);
    assert_eq!(
        providers
            .inspect_providers(&actor, workspace.id())
            .await
            .unwrap(),
        std::slice::from_ref(&restored),
        "restoration keeps the Provider identity"
    );
    assert_eq!(
        searchable_providers(&database, workspace.id()).await,
        [provider.call_root().clone()]
    );
    let before_delete = providers
        .list_lifecycle_events(&actor, workspace.id())
        .await
        .unwrap();
    assert_eq!(
        added_events(&after_disable, &before_delete),
        [(
            LifecycleAction::ProviderRestored,
            LifecycleOutcome::Accepted
        )]
    );
    let mut tx = database.begin().await.unwrap();
    assert_eq!(
        coral_provider::verify_invocation_operation_in_transaction(
            &mut tx,
            workspace.id(),
            &retained,
            coral_db::RowLock::Shared,
        )
        .await
        .unwrap(),
        InvocationOperationVerification::Current,
        "restoration makes retained work usable again"
    );
    tx.rollback().await.unwrap();

    providers
        .delete_provider(
            &context(&actor).with_etag(&restored.etag()).unwrap(),
            workspace.id(),
            provider.id(),
        )
        .await
        .unwrap();
    assert!(
        providers
            .inspect_providers(&actor, workspace.id())
            .await
            .unwrap()
            .is_empty()
    );
    let retained_after_deletion = async || {
        let mut tx = database.begin().await.unwrap();
        let verification = coral_provider::verify_invocation_operation_in_transaction(
            &mut tx,
            workspace.id(),
            &retained,
            coral_db::RowLock::Shared,
        )
        .await
        .unwrap();
        tx.rollback().await.unwrap();
        verification
    };
    assert_eq!(
        retained_after_deletion().await,
        InvocationOperationVerification::AuthorityLost
    );
    assert!(matches!(
        providers
            .delete_provider(&context(&actor), workspace.id(), provider.id())
            .await,
        Err(coral_provider::Error::ProviderNotFound)
    ));
    assert!(matches!(
        providers
            .restore_provider(&context(&actor), workspace.id(), provider.id())
            .await,
        Err(coral_provider::Error::ProviderNotFound)
    ));
    assert_eq!(
        providers
            .get_publication(&actor, workspace.id(), request.request_id())
            .await
            .unwrap(),
        publication,
        "deletion keeps the Publication attribution"
    );
    assert_eq!(
        providers
            .publish_workspace_provider(&context(&actor), request)
            .await
            .unwrap(),
        publication
    );
    assert_eq!(
        added_events(
            &before_delete,
            &providers
                .list_lifecycle_events(&actor, workspace.id())
                .await
                .unwrap()
        ),
        [(LifecycleAction::ProviderDeleted, LifecycleOutcome::Accepted)],
        "the refused deletion, restoration and replay record nothing"
    );
    let replacement_setup = self::setup(&providers, &actor, workspace.id()).await;
    let replacement = providers
        .publish_workspace_provider(
            &context(&actor),
            self::request(
                workspace.id(),
                &replacement_setup,
                "weather",
                "81000000-0000-4000-8000-000000000002",
            ),
        )
        .await
        .unwrap();
    assert_ne!(
        replacement.published_provider().unwrap().id(),
        provider.id()
    );
    assert_eq!(
        replacement
            .published_provider()
            .unwrap()
            .account_setting_version(),
        1
    );
    assert_eq!(
        retained_after_deletion().await,
        InvocationOperationVerification::AuthorityLost,
        "a new Provider in the namespace does not revive retained work"
    );
    let replacement_events = providers
        .list_lifecycle_events(&actor, workspace.id())
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.target_id() == replacement.id().to_string())
        .map(|event| (event.action(), event.outcome()))
        .collect::<Vec<_>>();
    assert_eq!(
        replacement_events,
        [(
            coral_provider::LifecycleAction::PublicationCompleted,
            coral_provider::LifecycleOutcome::Succeeded,
        )]
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one domain journey preserves authority evidence across dependent lifecycle transitions"
)]
async fn publication_request_keys_are_workspace_scoped_and_actor_bound() {
    let (db, workspaces, providers, actor) = harness().await;
    let one = workspaces
        .create(
            &context(&actor),
            WorkspaceDisplayName::parse("One").unwrap(),
        )
        .await
        .unwrap();
    let two = workspaces
        .create(
            &context(&actor),
            WorkspaceDisplayName::parse("Two").unwrap(),
        )
        .await
        .unwrap();
    let first = setup(&providers, &actor, one.id()).await;
    let second = setup(&providers, &actor, two.id()).await;
    let key = "82000000-0000-4000-8000-000000000001";
    let request_one = request(one.id(), &first, "weather", key);
    let a = providers
        .publish_workspace_provider(&context(&actor), request_one.clone())
        .await
        .unwrap();
    let b = providers
        .publish_workspace_provider(&context(&actor), request(two.id(), &second, "weather", key))
        .await
        .unwrap();
    assert_ne!(
        a.published_provider().unwrap().id(),
        b.published_provider().unwrap().id()
    );
    assert_eq!(
        providers
            .publish_workspace_provider(&context(&actor), request_one.clone())
            .await
            .unwrap(),
        a
    );
    let other = provision_user(db.database(), "user:other-owner", OrganizationRole::Owner).await;
    workspaces
        .assign_owner(&context(&other), one.id(), other.user_id())
        .await
        .unwrap();
    assert!(matches!(
        providers
            .publish_workspace_provider(&context(&other), request_one)
            .await,
        Err(coral_provider::Error::RequestIdConflict)
    ));
    assert!(matches!(
        providers
            .publish_workspace_provider(&context(&actor), request(one.id(), &first, "changed", key))
            .await,
        Err(coral_provider::Error::RequestIdConflict)
    ));
    assert!(matches!(
        providers
            .inspect_provider(&actor, two.id(), a.published_provider().unwrap().id())
            .await,
        Err(coral_provider::Error::ProviderNotFound)
    ));

    let replacement = setup(&providers, &actor, one.id()).await;
    let revision = providers
        .review_setup_configuration(&actor, one.id(), replacement.id())
        .await
        .unwrap()
        .revision;
    let update = ProviderRegenerationRequest::new(
        ManagementRequestId::parse(key).unwrap(),
        one.id().clone(),
        a.published_provider().unwrap().id().clone(),
        replacement.id().clone(),
        revision,
    );
    assert!(matches!(
        providers
            .regenerate_provider(&context(&actor), update)
            .await,
        Err(coral_provider::Error::RequestIdConflict)
    ));
    let update_key = "82000000-0000-4000-8000-000000000002";
    let update = ProviderRegenerationRequest::new(
        ManagementRequestId::parse(update_key).unwrap(),
        one.id().clone(),
        a.published_provider().unwrap().id().clone(),
        replacement.id().clone(),
        revision,
    );
    let committed = providers
        .regenerate_provider(&context(&actor), update.clone())
        .await
        .unwrap();
    assert_eq!(committed.outcome(), ProviderPublicationOutcome::Succeeded);
    assert_eq!(
        providers
            .regenerate_provider(&context(&actor), update.clone())
            .await
            .unwrap(),
        committed
    );
    assert!(matches!(
        providers
            .regenerate_provider(&context(&other), update)
            .await,
        Err(coral_provider::Error::RequestIdConflict)
    ));
    let next = setup(&providers, &actor, one.id()).await;
    assert!(matches!(
        providers
            .publish_workspace_provider(
                &context(&actor),
                request(one.id(), &next, "next", update_key)
            )
            .await,
        Err(coral_provider::Error::RequestIdConflict)
    ));
    let second_update_setup = setup(&providers, &actor, two.id()).await;
    let second_revision = providers
        .review_setup_configuration(&actor, two.id(), second_update_setup.id())
        .await
        .unwrap()
        .revision;
    let independent = providers
        .regenerate_provider(
            &context(&actor),
            ProviderRegenerationRequest::new(
                ManagementRequestId::parse(update_key).unwrap(),
                two.id().clone(),
                b.published_provider().unwrap().id().clone(),
                second_update_setup.id().clone(),
                second_revision,
            ),
        )
        .await
        .unwrap();
    assert_eq!(independent.outcome(), ProviderPublicationOutcome::Succeeded);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one domain journey preserves authority evidence across dependent lifecycle transitions"
)]
async fn existing_caller_binding_uses_the_workspace_provider_address() {
    use coral_credentials::{CredentialVault, Secret};
    use coral_provider::account::{AccountModule, AccountScope};
    let (db, workspaces, providers, actor) = harness().await;
    let database = db.database();
    let vault_directory = tempfile::tempdir().unwrap();
    let accounts = AccountModule::new(
        providers.clone(),
        CredentialVault::open(&vault_directory.path().join("key")).unwrap(),
    );
    let workspace = workspaces
        .create(
            &context(&actor),
            WorkspaceDisplayName::parse("Caller").unwrap(),
        )
        .await
        .unwrap();
    let source = SOURCE.replace("\"security\":[]", "\"security\":[{\"bearer\":[]}],\"components\":{\"securitySchemes\":{\"bearer\":{\"type\":\"http\",\"scheme\":\"bearer\"}}}");
    let setup = providers
        .create_openapi_setup(
            &context(&actor),
            workspace.id(),
            OpenApiDocument::parse(&source).unwrap(),
        )
        .await
        .unwrap();
    let review = providers
        .review_setup_configuration(&actor, workspace.id(), setup.id())
        .await
        .unwrap();
    providers
        .confirm_setup_configuration(
            &context(&actor),
            workspace.id(),
            setup.id(),
            review.revision,
            review.document.unwrap().suggested,
        )
        .await
        .unwrap();
    let publication = providers
        .publish_workspace_provider(
            &context(&actor),
            WorkspaceProviderPublicationRequest::new(
                ManagementRequestId::parse("84000000-0000-4000-8000-000000000001").unwrap(),
                workspace.id().clone(),
                setup.id().clone(),
                coral_provider::ProviderNamespace::parse("weather").unwrap(),
                CredentialRoute::Caller,
            ),
        )
        .await
        .unwrap();
    let provider = publication.published_provider().unwrap();
    let authorization = accounts
        .start_user_authorization(
            &context(&actor),
            workspace.id(),
            provider.id(),
            ManagementRequestId::parse("84000000-0000-4000-8000-000000000002").unwrap(),
            None,
        )
        .await
        .unwrap();
    let connected = accounts
        .submit_secret(
            &context(&actor),
            workspace.id(),
            authorization.id,
            coral_credentials::CredentialValues::token(
                Secret::new("dummy-caller-secret".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    let account = accounts
        .get_account(
            &context(&actor),
            workspace.id(),
            provider.id(),
            &AccountScope::User,
        )
        .await
        .unwrap()
        .account
        .unwrap();
    assert_eq!(Some(account.id), connected.account_id);
    assert_eq!(account.state, coral_provider::account::AccountState::Ready);
    let account_id = connected.account_id.unwrap();
    assert!(
        !accounts
            .credential_status(&context(&actor), workspace.id(), provider.id(), account_id)
            .await
            .unwrap()
            .requires_authorization
    );
    let other = provision_user(
        database.clone(),
        "user:other-account-owner",
        OrganizationRole::Owner,
    )
    .await;
    workspaces
        .assign_owner(&context(&other), workspace.id(), other.user_id())
        .await
        .unwrap();
    assert!(
        accounts
            .get_account(
                &context(&other),
                workspace.id(),
                provider.id(),
                &AccountScope::User
            )
            .await
            .unwrap()
            .account
            .is_none()
    );
    assert!(matches!(
        accounts
            .credential_status(&context(&other), workspace.id(), provider.id(), account_id)
            .await,
        Err(coral_provider::Error::AccountNotFound)
    ));
    let private_events = accounts
        .list_user_lifecycle_events(&context(&actor), workspace.id())
        .await
        .unwrap();
    assert!(!private_events.is_empty());
    assert!(
        accounts
            .list_user_lifecycle_events(&context(&other), workspace.id())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        providers
            .list_lifecycle_events(&other, workspace.id())
            .await
            .unwrap()
            .iter()
            .all(|event| event.target_id() != account_id.to_string())
    );
    let reconnect = accounts
        .start_reconnect(
            &context(&actor),
            workspace.id(),
            provider.id(),
            account_id,
            ManagementRequestId::parse("84000000-0000-4000-8000-000000000003").unwrap(),
            None,
        )
        .await
        .unwrap();
    let reconnected = accounts
        .submit_secret(
            &context(&actor),
            workspace.id(),
            reconnect.id,
            coral_credentials::CredentialValues::token(
                Secret::new("dummy-reconnected-secret".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(reconnected.account_id, Some(account_id));
    assert_eq!(
        accounts
            .get_account(
                &context(&actor),
                workspace.id(),
                provider.id(),
                &AccountScope::User
            )
            .await
            .unwrap()
            .account
            .map(|account| account.id),
        Some(account_id)
    );
    let mut tx = database.begin().await.unwrap();
    let retained = coral_provider::resolve_owner_operation_in_transaction(
        &mut tx,
        workspace.id(),
        &format!("{}.forecast", provider.call_root().as_str()),
    )
    .await
    .unwrap()
    .unwrap();
    let principal = coral_workspace::AuthenticatedPrincipal::user(actor.clone());
    assert!(
        coral_provider::account::select_in_transaction(&mut tx, &retained, principal.principal())
            .await
            .unwrap()
            .is_some()
    );
    tx.rollback().await.unwrap();
    providers
        .delete_provider(&context(&actor), workspace.id(), provider.id())
        .await
        .unwrap();
    assert!(matches!(
        accounts
            .get_account(
                &context(&actor),
                workspace.id(),
                provider.id(),
                &AccountScope::User
            )
            .await,
        Err(coral_provider::Error::ProviderNotFound)
    ));
}

/// Everything a Workspace Owner can observe about one Workspace's Providers.
#[derive(Debug, PartialEq)]
struct WorkspaceState {
    providers: Vec<coral_provider::Provider>,
    events: Vec<(LifecycleAction, LifecycleOutcome, String)>,
    setups: Vec<coral_provider::ProviderSetupState>,
    account: Option<coral_provider::account::AccountSummary>,
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one sweep drives every Workspace-scoped mutation against the same observed state"
)]
async fn foreign_workspace_mutations_are_denied_and_change_nothing() {
    use coral_credentials::{CredentialVault, Secret};
    use coral_provider::account::{AccountModule, AccountScope};
    let (_db, workspaces, providers, actor) = harness().await;
    let vault_directory = tempfile::tempdir().expect("vault directory");
    let accounts = AccountModule::new(
        providers.clone(),
        CredentialVault::open(&vault_directory.path().join("key")).expect("vault"),
    );
    let home = workspaces
        .create(
            &context(&actor),
            WorkspaceDisplayName::parse("Home").unwrap(),
        )
        .await
        .unwrap();
    let foreign = workspaces
        .create(
            &context(&actor),
            WorkspaceDisplayName::parse("Foreign").unwrap(),
        )
        .await
        .unwrap();
    // A Caller-route Provider with a connected Account, an update Setup and
    // a pending Setup, all in the home Workspace.
    let source = SOURCE.replace("\"security\":[]", "\"security\":[{\"bearer\":[]}],\"components\":{\"securitySchemes\":{\"bearer\":{\"type\":\"http\",\"scheme\":\"bearer\"}}}");
    let published = providers
        .create_openapi_setup(
            &context(&actor),
            home.id(),
            OpenApiDocument::parse(&source).unwrap(),
        )
        .await
        .unwrap();
    let review = providers
        .review_setup_configuration(&actor, home.id(), published.id())
        .await
        .unwrap();
    providers
        .confirm_setup_configuration(
            &context(&actor),
            home.id(),
            published.id(),
            review.revision,
            review.document.unwrap().suggested,
        )
        .await
        .unwrap();
    let provider = providers
        .publish_workspace_provider(
            &context(&actor),
            WorkspaceProviderPublicationRequest::new(
                ManagementRequestId::parse("85000000-0000-4000-8000-000000000001").unwrap(),
                home.id().clone(),
                published.id().clone(),
                coral_provider::ProviderNamespace::parse("weather").unwrap(),
                CredentialRoute::Caller,
            ),
        )
        .await
        .unwrap()
        .published_provider()
        .unwrap()
        .clone();
    let authorization = accounts
        .start_user_authorization(
            &context(&actor),
            home.id(),
            provider.id(),
            ManagementRequestId::parse("85000000-0000-4000-8000-000000000002").unwrap(),
            None,
        )
        .await
        .unwrap();
    let account = accounts
        .submit_secret(
            &context(&actor),
            home.id(),
            authorization.id,
            coral_credentials::CredentialValues::token(
                Secret::new("dummy-sweep-secret".into()).unwrap(),
            ),
        )
        .await
        .unwrap()
        .account_id
        .unwrap();
    let pending = setup(&providers, &actor, home.id()).await;
    let update = setup(&providers, &actor, home.id()).await;
    let update_revision = providers
        .review_setup_configuration(&actor, home.id(), update.id())
        .await
        .unwrap()
        .revision;

    let observe = async |workspace: &WorkspaceId| WorkspaceState {
        providers: providers
            .inspect_providers(&actor, workspace)
            .await
            .unwrap(),
        events: providers
            .list_lifecycle_events(&actor, workspace)
            .await
            .unwrap()
            .iter()
            .map(|event| {
                (
                    event.action(),
                    event.outcome(),
                    event.target_id().to_owned(),
                )
            })
            .collect(),
        setups: {
            let mut states = Vec::new();
            for setup in [&published, &pending, &update] {
                states.push(
                    providers
                        .get_setup(&actor, home.id(), setup.id())
                        .await
                        .unwrap()
                        .state(),
                );
            }
            states
        },
        account: accounts
            .get_account(
                &context(&actor),
                home.id(),
                provider.id(),
                &AccountScope::User,
            )
            .await
            .unwrap()
            .account,
    };
    let (home_before, foreign_before) = (observe(home.id()).await, observe(foreign.id()).await);
    let foreign_id = foreign.id();
    let denials = [
        (
            "disable",
            providers
                .disable_provider(&context(&actor), foreign_id, provider.id())
                .await
                .map(drop),
            "ProviderNotFound",
        ),
        (
            "restore",
            providers
                .restore_provider(&context(&actor), foreign_id, provider.id())
                .await
                .map(drop),
            "ProviderNotFound",
        ),
        (
            "delete",
            providers
                .delete_provider(&context(&actor), foreign_id, provider.id())
                .await,
            "ProviderNotFound",
        ),
        (
            "route change",
            providers
                .prepare_setup(
                    &context(&actor),
                    foreign_id,
                    coral_provider::ProviderSetupSource::Current,
                    Some(provider.id()),
                )
                .await
                .map(drop),
            "ProviderNotFound",
        ),
        (
            "regenerate",
            providers
                .regenerate_provider(
                    &context(&actor),
                    ProviderRegenerationRequest::new(
                        ManagementRequestId::parse("85000000-0000-4000-8000-000000000003").unwrap(),
                        foreign_id.clone(),
                        provider.id().clone(),
                        update.id().clone(),
                        update_revision,
                    )
                    .with_account_setting(CredentialRoute::Fixed),
                )
                .await
                .map(drop),
            "ProviderNotFound",
        ),
        (
            "disconnect",
            accounts
                .disconnect(&context(&actor), foreign_id, provider.id(), account)
                .await,
            "AccountNotFound",
        ),
        (
            "cancel setup",
            accounts
                .cancel_setup(&context(&actor), foreign_id, pending.id())
                .await
                .map(drop),
            "ProviderSetupNotFound",
        ),
    ];
    for (mutation, result, denial) in denials {
        assert_eq!(
            result.map_err(|error| format!("{error:?}")),
            Err(denial.to_owned()),
            "{mutation}"
        );
    }
    assert_eq!(observe(home.id()).await, home_before);
    assert_eq!(observe(foreign.id()).await, foreign_before);
}

/// Every surface that shows a Provider display name gets it from this parse,
/// so invisible reordering and control characters never reach one of them.
#[tokio::test]
async fn display_names_reject_control_and_format_characters() {
    for (case, name) in [
        ("control", "Weather\u{0007}"),
        ("bidi override", "Weather\u{202E}lmth.exe"),
        ("bidi isolate", "\u{2066}Weather\u{2069}"),
        ("zero-width joiner", "Wea\u{200D}ther"),
        ("soft hyphen", "Wea\u{00AD}ther"),
        ("byte order mark", "\u{FEFF}Weather"),
    ] {
        assert!(
            matches!(
                ProviderDisplayName::parse(name),
                Err(coral_provider::Error::InvalidInput(_))
            ),
            "{case}"
        );
    }
    assert_eq!(
        ProviderDisplayName::parse("Météo ☀️ 天气")
            .unwrap()
            .as_str(),
        "Météo ☀️ 天气"
    );

    let (_db, workspaces, providers, actor) = harness().await;
    let workspace = workspaces
        .create(
            &context(&actor),
            WorkspaceDisplayName::parse("Weather").unwrap(),
        )
        .await
        .unwrap();
    let setup = setup_from(
        &providers,
        &actor,
        workspace.id(),
        &SOURCE.replace(
            "\"title\":\"Weather\"",
            "\"title\":\"Weather\\u202Elmth.exe\"",
        ),
    )
    .await;
    let events = providers
        .list_lifecycle_events(&actor, workspace.id())
        .await
        .unwrap();
    let rejected = providers
        .publish_workspace_provider(
            &context(&actor),
            request(
                workspace.id(),
                &setup,
                "weather",
                "86000000-0000-4000-8000-000000000001",
            ),
        )
        .await;
    assert!(
        matches!(rejected, Err(coral_provider::Error::InvalidInput(_))),
        "{rejected:?}"
    );
    assert!(
        providers
            .inspect_providers(&actor, workspace.id())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        providers
            .list_lifecycle_events(&actor, workspace.id())
            .await
            .unwrap(),
        events
    );
}
