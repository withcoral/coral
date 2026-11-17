//! Publication must recheck provisional deadlines after asynchronous materialization.
use super::*;
use crate::account_tests::{CREDENTIAL_REQUIRED_OPENAPI, authorization_request};
use coral_credentials::Secret;

/// How long before its grant's deadline the held Publication's request is
/// dated.
const REMAINING: Duration = Duration::from_millis(50);

/// A Publication that promotes a Setup's grant, after the Module's clock
/// measured a span across its materialization.
struct Published {
    /// Owns the database: bind it for as long as a test reads `providers`.
    _fixture: coral_test_support::TestDatabase,
    providers: ProviderModule,
    actor: AuthenticatedUser,
    workspace: WorkspaceId,
    setup: crate::ProviderSetupId,
    /// When the grant, and the Setup it belongs to, expire.
    deadline: i64,
    request: WorkspaceProviderPublicationRequest,
    /// The request's instant: [`REMAINING`] before `deadline`.
    context: coral_workspace::ChangeContext,
    result: crate::ProviderPublication,
}

/// Publishes a Setup with a sealed grant, on a request dated [`REMAINING`]
/// before the grant's deadline, and moves the Module's clock on by `span`
/// while the materializer holds the Publication. Nothing else moves the
/// Publication's time: the test waits for nothing.
async fn publish_across(span: Duration) -> Published {
    let (fixture, database, workspaces) = test_foundation().await;
    let actor = organization_user(
        fixture.database(),
        "user:publication-expiry",
        OrganizationRole::Owner,
    )
    .await;
    let context = change_context(actor.clone(), 20, '1');
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Expiry").unwrap())
        .await
        .unwrap();
    let materializer = TestMaterializer::gated();
    // The clock's reading is no request's instant: only the span between two
    // of its readings dates the Publication.
    let clock = coral_test_support::ManualClock::starting_at(coral_db::UnixNanos::new(7));
    let providers = ProviderModule::with_test_dependencies(
        database,
        workspaces,
        materializer.clone(),
        Arc::new(NoopPublicationObserver),
    )
    .with_clock(Arc::new(clock.clone()));
    let directory = tempfile::tempdir().unwrap();
    let accounts = crate::tests::accounts_for(&providers, directory.path());
    let setup = providers
        .prepare_openapi_fixture(
            &context,
            workspace.id(),
            OpenApiDocument::parse(CREDENTIAL_REQUIRED_OPENAPI).unwrap(),
        )
        .await
        .unwrap();
    let grant = accounts
        .authorize_setup(
            &context,
            workspace.id(),
            setup.id(),
            1,
            coral_credentials::CredentialValues::token(
                Secret::new("expiring-grant-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    let deadline = grant.expires_at_unix_nanos.unwrap();
    let request = WorkspaceProviderPublicationRequest::new(
        authorization_request(),
        workspace.id().clone(),
        setup.id().clone(),
        crate::ProviderNamespace::parse("expiring").unwrap(),
        CredentialRoute::Fixed,
    )
    .with_account_grant(grant.provisional_grant_id.unwrap());
    let remaining = i64::try_from(REMAINING.as_nanos()).unwrap();
    let before_expiry = change_context(actor.clone(), deadline - remaining, '2');
    let task = {
        let providers = providers.clone();
        let request = request.clone();
        let context = before_expiry.clone();
        tokio::spawn(async move {
            providers
                .publish_workspace_provider(&context, request)
                .await
                .map(crate::Committed::skip_search)
        })
    };
    tokio::time::timeout(Duration::from_secs(5), materializer.held())
        .await
        .unwrap();
    // The materializer has its answer. Only the clock says how long it took.
    clock.advance(span);
    materializer.release();
    let result = task.await.unwrap().unwrap();
    Published {
        _fixture: fixture,
        providers,
        actor,
        workspace: workspace.id().clone(),
        setup: setup.id().clone(),
        deadline,
        request,
        context: before_expiry,
        result,
    }
}

/// The Module's clock dates the Publication's decisions: a materialization
/// that ends at the grant's deadline publishes nothing.
#[tokio::test]
async fn account_grant_expiring_during_materialization_is_not_promoted() {
    let Published {
        providers,
        actor,
        workspace,
        setup,
        deadline,
        request,
        context: before_expiry,
        result,
        _fixture,
    } = publish_across(REMAINING).await;
    assert_eq!(result.outcome(), ProviderPublicationOutcome::Failed);
    assert_eq!(result.diagnostic().unwrap().code(), "setup_expired");
    assert!(result.published_provider().is_none());
    assert_eq!(
        providers
            .inspect_providers(&actor, &workspace)
            .await
            .unwrap(),
        [] as [crate::Provider; 0]
    );
    let events = providers
        .list_lifecycle_events(&actor, &workspace)
        .await
        .unwrap();
    assert!(
        events
            .iter()
            .all(|event| event.action() != LifecycleAction::AccountConnected)
    );
    assert!(
        !providers
            .setup_authentication(&actor, &workspace, &setup, deadline + 1)
            .await
            .unwrap()
            .authorized
    );
    assert_eq!(
        providers
            .publish_workspace_provider(&before_expiry, request)
            .await
            .unwrap()
            .skip_search(),
        result
    );
    assert_eq!(
        providers
            .list_lifecycle_events(&actor, &workspace)
            .await
            .unwrap(),
        events
    );
}

/// One nanosecond short of the grant's deadline, the same Publication
/// promotes the grant: the span the clock measured decides, and no other
/// time does.
#[tokio::test]
async fn account_grant_live_at_the_end_of_materialization_is_promoted() {
    let Published {
        providers,
        actor,
        workspace,
        result,
        _fixture,
        ..
    } = publish_across(REMAINING.checked_sub(Duration::from_nanos(1)).unwrap()).await;
    assert_eq!(result.outcome(), ProviderPublicationOutcome::Succeeded);
    let published = result.published_provider().unwrap();
    assert_eq!(
        providers
            .inspect_providers(&actor, &workspace)
            .await
            .unwrap()
            .iter()
            .map(crate::Provider::id)
            .collect::<Vec<_>>(),
        [published.id()]
    );
    assert!(
        providers
            .list_lifecycle_events(&actor, &workspace)
            .await
            .unwrap()
            .iter()
            .any(|event| event.action() == LifecycleAction::AccountConnected)
    );
}
