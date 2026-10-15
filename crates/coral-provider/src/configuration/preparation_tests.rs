//! Preparation reuse through the same Module interface used by management adapters.
use std::time::Duration;

use coral_catalog::{AuthenticationConfiguration, OpenApiDocument};
use coral_outbound::DestinationPolicy;

use crate::access::CredentialRoute;
use crate::publication::materialization::TestMaterializer;
use crate::setup_tests::{harness, workspace_owner};
use crate::tests::{change_context, publication_request};
use crate::{Error, ProviderModule, ProviderPublication, ProviderPublicationOutcome};

fn document() -> OpenApiDocument {
    OpenApiDocument::parse(
        r"
openapi: 3.0.3
info: {title: Preparation, version: '1'}
security: []
servers: [{url: 'https://api.example.test'}]
paths:
  /data:
    get:
      operationId: getData
      responses:
        '200': {description: OK}
",
    )
    .unwrap()
}

#[tokio::test]
async fn preparation_is_reused_by_reads_replays_invalid_edits_and_restarts() {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "user:preparation-reuse").await;
    let context = change_context(actor.clone(), 10, '1');
    let owner = workspace.id().clone();
    let count = TestMaterializer::counting();
    let mut providers = h.providers.clone();
    providers.preparation = providers.preparation.with_materializer(count.clone());
    let setup = providers
        .create_openapi_setup(&context, workspace.id(), document())
        .await
        .unwrap();
    let original = providers
        .review_setup_configuration(&actor, &owner, setup.id())
        .await
        .unwrap();
    let values = original.document.unwrap().suggested;
    for _ in 0..3 {
        providers
            .inspect_setup(&actor, &owner, setup.id(), 11)
            .await
            .unwrap();
    }
    assert_eq!(count.analyses(), 1);
    assert_eq!(count.calls(), 0);

    let confirmed = providers
        .confirm_setup_configuration(&context, &owner, setup.id(), 0, values.clone())
        .await
        .unwrap();
    assert!(confirmed.errors.is_empty());
    assert_eq!(
        confirmed.destinations.get("getData").map(String::as_str),
        Some("https://api.example.test/")
    );
    assert_eq!(
        count.calls(),
        1,
        "confirmation must share one catalog between contract and review"
    );

    for revision in [0, 1] {
        let replay = providers
            .confirm_setup_configuration(&context, &owner, setup.id(), revision, values.clone())
            .await
            .unwrap();
        assert_eq!(replay, confirmed);
    }
    let mut invalid = values.clone();
    invalid.authentication = None;
    let rejected = providers
        .confirm_setup_configuration(&context, &owner, setup.id(), 1, invalid)
        .await
        .unwrap();
    assert!(!rejected.errors.is_empty());
    assert_eq!(rejected.revision, 1);
    assert_eq!(rejected.destinations, confirmed.destinations);
    assert_eq!(count.calls(), 1);

    // A new Module has no in-memory results from preparation, even on another host process.
    let mut restarted = ProviderModule::new(
        DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
        h.database.clone(),
        h.workspaces.clone(),
    );
    restarted.preparation = restarted.preparation.with_materializer(count.clone());
    let vault = tempfile::tempdir().unwrap();
    let accounts = crate::tests::accounts_for(&restarted, vault.path());
    for _ in 0..3 {
        let inspected = restarted
            .inspect_setup(&actor, &owner, setup.id(), 12)
            .await
            .unwrap();
        assert_eq!(inspected.configuration, confirmed);
        assert_eq!(
            inspected.authentication.contract.authentication(),
            &AuthenticationConfiguration::None
        );
        assert!(
            !accounts
                .setup_automatic_client_available(&actor, &owner, setup.id())
                .await
                .unwrap()
        );
    }
    assert_eq!(count.analyses(), 1, "the restarted Module analyzes nothing");
    assert_eq!(count.calls(), 1);
    let mut changed = values;
    changed.display_name = "Renamed".into();
    let edited = restarted
        .confirm_setup_configuration(&context, &owner, setup.id(), 1, changed)
        .await
        .unwrap();
    assert_eq!(edited.revision, 2);
    assert_eq!(edited.destinations, confirmed.destinations);
    assert_eq!(count.calls(), 2);
    assert_eq!(count.analyses(), 1);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one update journey covers unknown compatibility, restart reuse, and compatible/incompatible confirmation"
)]
async fn update_compatibility_is_unknown_until_prepared_and_survives_restart() {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "user:update-compatibility").await;
    let context = change_context(actor.clone(), 10, '1');
    let owner = workspace.id().clone();
    let first = h
        .providers
        .create_openapi_setup(&context, workspace.id(), document())
        .await
        .unwrap();
    let mut values = h
        .providers
        .review_setup_configuration(&actor, &owner, first.id())
        .await
        .unwrap()
        .document
        .unwrap()
        .suggested;
    values.authentication = Some(AuthenticationConfiguration::Bearer);
    h.providers
        .confirm_setup_configuration(&context, &owner, first.id(), 0, values)
        .await
        .unwrap();
    let published = h
        .providers
        .publish_workspace_provider(
            &context,
            crate::WorkspaceProviderPublicationRequest::new(
                crate::ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
                workspace.id().clone(),
                first.id().clone(),
                crate::access::ProviderNamespace::parse("compatibility").unwrap(),
                crate::access::CredentialRoute::Caller,
            ),
        )
        .await
        .unwrap();
    let provider = published.published_provider().unwrap();
    let count = TestMaterializer::counting();
    let mut providers = h.providers.clone();
    providers.preparation = providers.preparation.with_materializer(count.clone());
    let replacement = providers
        .prepare_setup(
            &context,
            &owner,
            crate::ProviderSetupSource::OpenApiDocument(document()),
            Some(provider.id()),
        )
        .await
        .unwrap();
    let initial = providers
        .inspect_setup(&actor, &owner, replacement.id(), 11)
        .await
        .unwrap();
    assert!(!initial.configuration.configuration_confirmed);
    assert_eq!(
        initial.configuration.interrupts_access, None,
        "missing preparation is unknown, not anonymous or compatible"
    );
    assert_eq!(count.calls(), 0, "Update source review must remain cheap");
    let mut restarted = ProviderModule::new(
        DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
        h.database.clone(),
        h.workspaces.clone(),
    );
    restarted.preparation = restarted.preparation.with_materializer(count.clone());
    let review = restarted
        .review_setup_configuration(&actor, &owner, replacement.id())
        .await
        .unwrap();
    assert_eq!(review, initial.configuration);
    let retained = review.confirmed.unwrap();
    let confirmed = restarted
        .confirm_setup_configuration(
            &context,
            &owner,
            replacement.id(),
            review.revision,
            retained.clone(),
        )
        .await
        .unwrap();
    assert!(confirmed.errors.is_empty());
    assert_eq!(confirmed.interrupts_access, Some(false));
    assert_eq!(confirmed.destinations.len(), 1);
    assert_eq!(count.calls(), 1);
    assert_eq!(
        providers
            .review_setup_configuration(&actor, &owner, replacement.id())
            .await
            .unwrap(),
        confirmed
    );
    let mut changed = retained;
    changed.servers.insert(
        "#/servers".into(),
        coral_catalog::ServerSelection::Custom {
            url: "https://api.example.test/new".into(),
        },
    );
    let incompatible = providers
        .confirm_setup_configuration(
            &context,
            &owner,
            replacement.id(),
            confirmed.revision,
            changed,
        )
        .await
        .unwrap();
    assert!(incompatible.errors.is_empty());
    assert_eq!(incompatible.interrupts_access, Some(true));
    assert_eq!(
        restarted
            .review_setup_configuration(&actor, &owner, replacement.id())
            .await
            .unwrap(),
        incompatible
    );
    assert_eq!(count.calls(), 2);
}

#[tokio::test]
async fn failed_preparation_accepts_nothing_and_can_be_retried() {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "user:preparation-failure").await;
    let context = change_context(actor.clone(), 10, '1');
    let owner = workspace.id().clone();
    let setup = h
        .providers
        .create_openapi_setup(&context, workspace.id(), document())
        .await
        .unwrap();
    let before = h
        .providers
        .review_setup_configuration(&actor, &owner, setup.id())
        .await
        .unwrap();
    let values = before.document.as_ref().unwrap().suggested.clone();
    let mut failed = h.providers.clone();
    failed.preparation = failed
        .preparation
        .with_materializer(TestMaterializer::failing_once());
    assert!(matches!(
        failed
            .confirm_setup_configuration(&context, &owner, setup.id(), 0, values.clone())
            .await,
        Err(Error::SetupPreparationUnavailable)
    ));
    assert_eq!(
        h.providers
            .review_setup_configuration(&actor, &owner, setup.id())
            .await
            .unwrap(),
        before
    );
    let retry = h
        .providers
        .confirm_setup_configuration(&context, &owner, setup.id(), 0, values)
        .await
        .unwrap();
    assert!(retry.errors.is_empty());
    assert!(retry.configuration_confirmed);
    assert_eq!(retry.revision, 1);
}

#[tokio::test]
async fn stale_preparation_cannot_overwrite_a_new_configuration() {
    preparation_race(false).await;
}

#[tokio::test]
async fn preparation_rechecks_workspace_authority_before_acceptance() {
    preparation_race(true).await;
}

async fn preparation_race(archive: bool) {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "user:preparation-race").await;
    let context = change_context(actor.clone(), 10, '1');
    let owner = workspace.id().clone();
    let setup = h
        .providers
        .create_openapi_setup(&context, workspace.id(), document())
        .await
        .unwrap();
    let values = h
        .providers
        .review_setup_configuration(&actor, &owner, setup.id())
        .await
        .unwrap()
        .document
        .unwrap()
        .suggested;
    let gate = TestMaterializer::gated();
    let mut slow = h.providers.clone();
    slow.preparation = slow.preparation.with_materializer(gate.clone());
    let confirming = {
        let context = context.clone();
        let owner = owner.clone();
        let id = setup.id().clone();
        let values = values.clone();
        tokio::spawn(async move {
            slow.confirm_setup_configuration(&context, &owner, &id, 0, values)
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), gate.held())
        .await
        .unwrap();
    if archive {
        let archived = tokio::time::timeout(
            Duration::from_secs(5),
            h.workspaces.archive(&context, workspace.id()),
        )
        .await;
        gate.release();
        archived
            .expect("preparation must not prevent archiving")
            .unwrap();
        assert!(matches!(
            confirming.await.unwrap(),
            Err(Error::Workspace(coral_workspace::Error::WorkspaceArchived))
        ));
        let inspected = h
            .providers
            .inspect_setup(&actor, &owner, setup.id(), 12)
            .await
            .unwrap();
        assert!(!inspected.configuration.configuration_confirmed);
        assert_eq!(inspected.configuration.revision, 0);
        assert!(inspected.configuration.destinations.is_empty());
        return;
    }
    let mut winner = values;
    winner.authentication = Some(AuthenticationConfiguration::Bearer);
    let completed = tokio::time::timeout(
        Duration::from_secs(5),
        h.providers
            .confirm_setup_configuration(&context, &owner, setup.id(), 0, winner),
    )
    .await;
    gate.release();
    let completed = completed
        .expect("preparation must hold no database transaction")
        .unwrap();
    assert!(completed.errors.is_empty());
    assert!(matches!(
        confirming.await.unwrap(),
        Err(Error::EtagMismatch)
    ));
    let inspected = h
        .providers
        .inspect_setup(&actor, &owner, setup.id(), 12)
        .await
        .unwrap();
    assert_eq!(inspected.configuration, completed);
    assert_eq!(
        inspected.authentication.contract.authentication(),
        &AuthenticationConfiguration::Bearer
    );
}

#[tokio::test]
async fn a_different_host_policy_cannot_reuse_prepared_authority() {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "user:preparation-policy").await;
    let context = change_context(actor.clone(), 10, '1');
    let owner = workspace.id().clone();
    let setup = h
        .providers
        .create_openapi_setup(&context, workspace.id(), document())
        .await
        .unwrap();
    let values = h
        .providers
        .review_setup_configuration(&actor, &owner, setup.id())
        .await
        .unwrap()
        .document
        .unwrap()
        .suggested;
    let mut values = values;
    values.servers.insert(
        "#/servers".into(),
        coral_catalog::ServerSelection::Custom {
            url: "http://127.0.0.1:12345".into(),
        },
    );
    let local = h
        .providers
        .confirm_setup_configuration(&context, &owner, setup.id(), 0, values)
        .await
        .unwrap();
    assert!(local.diagnostic.is_none());
    let mut hosted = ProviderModule::new(
        DestinationPolicy::default(),
        h.database.clone(),
        h.workspaces.clone(),
    );
    let count = TestMaterializer::counting();
    hosted.preparation = hosted.preparation.with_materializer(count.clone());
    let inspected = hosted
        .inspect_setup(&actor, &owner, setup.id(), 12)
        .await
        .unwrap();
    assert_eq!(
        inspected.configuration.diagnostic.as_deref(),
        Some("outbound_destination_rejected")
    );
    assert!(inspected.configuration.destinations.is_empty());
    assert!(inspected.authentication.contract.destinations().is_empty());
    assert_eq!(
        h.providers
            .review_setup_configuration(&actor, &owner, setup.id())
            .await
            .unwrap(),
        local
    );
    assert_eq!((count.analyses(), count.calls()), (0, 0));
}

#[derive(Debug, Clone, Copy)]
enum Publisher {
    /// The Module that confirmed the preview.
    Same,
    /// The same Module after later previews of other Setups.
    AfterOtherPreviews,
    /// A Module that never saw the preview, such as after a restart.
    Restarted,
    /// The same Module after another process confirmed new settings.
    ReconfiguredElsewhere,
}

async fn confirm_and_publish(
    publisher: Publisher,
    policy: DestinationPolicy,
    server: Option<&str>,
) -> (usize, ProviderPublication) {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "user:preview-reuse").await;
    let context = change_context(actor.clone(), 10, '1');
    let owner = workspace.id().clone();
    let count = TestMaterializer::counting();
    let mut providers = h.providers.clone();
    providers.outbound_policy = policy;
    providers.materializer = count.clone();
    let confirm = async |providers: &ProviderModule| {
        let setup = providers
            .create_openapi_setup(&context, &owner, document())
            .await
            .unwrap();
        let mut values = providers
            .review_setup_configuration(&actor, &owner, setup.id())
            .await
            .unwrap()
            .document
            .unwrap()
            .suggested;
        if let Some(url) = server {
            values.servers.insert(
                "#/servers".into(),
                coral_catalog::ServerSelection::Custom { url: url.into() },
            );
        }
        providers
            .confirm_setup_configuration(&context, &owner, setup.id(), 0, values.clone())
            .await
            .unwrap();
        (setup, values)
    };
    let (setup, values) = confirm(&providers).await;
    match publisher {
        Publisher::Same => {}
        Publisher::AfterOtherPreviews => {
            for _ in 0..4 {
                confirm(&providers).await;
            }
        }
        Publisher::Restarted => providers = crate::tests::forgetting_previews(providers),
        Publisher::ReconfiguredElsewhere => {
            let mut renamed = values;
            renamed.display_name = "Renamed".into();
            crate::tests::forgetting_previews(providers.clone())
                .confirm_setup_configuration(&context, &owner, setup.id(), 1, renamed)
                .await
                .unwrap();
        }
    }
    let publication = providers
        .publish_workspace_provider(
            &context,
            publication_request(
                &owner,
                setup.id(),
                "reused",
                CredentialRoute::None,
                &coral_test_support::fixture_request_id(),
            ),
        )
        .await
        .unwrap();
    (count.calls(), publication)
}

#[tokio::test]
async fn publication_reuses_only_a_kept_preview_catalog() {
    let loopback =
        DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps);
    for (publisher, materializations) in [
        (Publisher::Same, 0),
        (Publisher::AfterOtherPreviews, 1),
        (Publisher::Restarted, 1),
        (Publisher::ReconfiguredElsewhere, 1),
    ] {
        let (calls, publication) = confirm_and_publish(publisher, loopback, None).await;
        assert_eq!(calls, materializations, "{publisher:?}");
        let published = publication.published_provider().expect("{publisher:?}");
        let expected = match publisher {
            Publisher::ReconfiguredElsewhere => "Renamed",
            _ => "Preparation",
        };
        assert_eq!(published.display_name(), expected, "{publisher:?}");
    }
}

#[tokio::test]
async fn a_kept_preview_catalog_meets_the_host_policy() {
    // The preview permits loopback; this host does not.
    let (calls, publication) = confirm_and_publish(
        Publisher::Same,
        DestinationPolicy::default(),
        Some("http://127.0.0.1:12345"),
    )
    .await;
    assert_eq!(calls, 0);
    assert_eq!(publication.outcome(), ProviderPublicationOutcome::Failed);
    assert_eq!(
        publication.diagnostic().unwrap().code(),
        "outbound_destination_rejected"
    );
}
