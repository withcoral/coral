//! A committed Publication names what Universal Search must read again,
//! beside the unchanged durable result.
use super::*;

use crate::{ActivatedGeneration, SearchChange};

async fn active_generation(
    providers: &ProviderModule,
    workspace: &WorkspaceId,
    provider: &crate::ProviderId,
) -> coral_catalog::ProviderGenerationId {
    let mut transaction = providers
        .database
        .begin_read_snapshot()
        .await
        .expect("begin");
    let generation = crate::repository::current_provider(
        &mut transaction,
        workspace,
        crate::repository::ProviderKey::Id(provider),
        crate::repository::ProviderHold::None,
    )
    .await
    .expect("read Provider")
    .expect("Provider")
    .generation_id;
    transaction.rollback().await.expect("rollback");
    generation
}

#[tokio::test]
async fn a_creation_names_the_generation_it_activated_and_a_failure_names_none() {
    let (_fixture, workspaces, providers) = test_modules().await;
    let fixture = WorkspaceRegenerationFixture::new(
        &workspaces,
        &providers,
        "user:creation-evidence",
        "15000000-0000-4000-8000-000000000001",
    )
    .await;
    let active = active_generation(&providers, &fixture.workspace_id, &fixture.provider_id).await;
    let setup = fixture
        .replacement_setup(&providers, CREDENTIAL_FREE_OPENAPI, 40, '4')
        .await;
    let context = change_context(fixture.owner.clone(), 50, '5');
    let conflict = publication_request(
        &fixture.workspace_id,
        setup.id(),
        fixture.provider_access.call_root().name(),
        CredentialRoute::None,
        "15000000-0000-4000-8000-000000000002",
    );
    let failed = providers
        .publish_workspace_provider(&context, conflict)
        .await
        .expect("publish");
    assert_eq!(failed.value.outcome(), ProviderPublicationOutcome::Failed);
    assert_eq!(failed.search, SearchChange::Unchanged);

    let setup = fixture
        .replacement_setup(&providers, CREDENTIAL_FREE_OPENAPI, 60, '6')
        .await;
    let context = change_context(fixture.owner.clone(), 70, '7');
    let created = providers
        .publish_workspace_provider(
            &context,
            publication_request(
                &fixture.workspace_id,
                setup.id(),
                "second",
                CredentialRoute::None,
                "15000000-0000-4000-8000-000000000003",
            ),
        )
        .await
        .expect("publish");
    let provider = created
        .value
        .published_provider()
        .expect("created Provider");
    let generation = active_generation(&providers, &fixture.workspace_id, provider.id()).await;
    assert_ne!(generation, active);
    assert_eq!(
        created.search,
        SearchChange::Activated(ActivatedGeneration::new(
            fixture.workspace_id.clone(),
            provider.id().clone(),
            generation,
            None,
        ))
    );
}

#[tokio::test]
async fn a_regeneration_names_the_generation_it_replaced_and_a_reuse_or_failure_names_none() {
    enum Expected {
        Replaced,
        Reused,
        Failed,
    }
    let cases = [
        (
            "changed",
            UPDATED_CREDENTIAL_FREE_OPENAPI,
            Expected::Replaced,
        ),
        ("unchanged", CREDENTIAL_FREE_OPENAPI, Expected::Reused),
        (
            "structural failure",
            INVALID_VERSION_OPENAPI,
            Expected::Failed,
        ),
    ];
    for (case, document, expected) in cases {
        let (_fixture, workspaces, providers) = test_modules().await;
        let fixture = WorkspaceRegenerationFixture::new(
            &workspaces,
            &providers,
            "user:regeneration-evidence",
            "15000000-0000-4000-8000-000000000011",
        )
        .await;
        let before =
            active_generation(&providers, &fixture.workspace_id, &fixture.provider_id).await;
        let setup = fixture
            .replacement_setup(&providers, document, 40, '4')
            .await;
        let context = change_context(fixture.owner.clone(), 50, '5');
        let published = providers
            .regenerate_provider(
                &context,
                fixture.request(setup.id(), "15000000-0000-4000-8000-000000000012"),
            )
            .await
            .expect("regenerate");
        let after =
            active_generation(&providers, &fixture.workspace_id, &fixture.provider_id).await;
        let expected = match expected {
            Expected::Replaced => SearchChange::Activated(ActivatedGeneration::new(
                fixture.workspace_id.clone(),
                fixture.provider_id.clone(),
                after,
                Some(before),
            )),
            Expected::Reused => {
                assert_eq!(after, before, "{case}");
                SearchChange::Unchanged
            }
            Expected::Failed => {
                assert_eq!(
                    published.value.outcome(),
                    ProviderPublicationOutcome::Failed,
                    "{case}"
                );
                SearchChange::Unchanged
            }
        };
        assert_eq!(published.search, expected, "{case}");
    }
}

#[tokio::test]
async fn a_request_that_finds_its_committed_result_is_a_replay() {
    let (_fixture, workspaces, providers) = test_modules().await;
    let fixture = WorkspaceRegenerationFixture::new(
        &workspaces,
        &providers,
        "user:replay-evidence",
        "15000000-0000-4000-8000-000000000021",
    )
    .await;
    let setup = fixture
        .replacement_setup(&providers, UPDATED_CREDENTIAL_FREE_OPENAPI, 40, '4')
        .await;
    let request = fixture.request(setup.id(), "15000000-0000-4000-8000-000000000022");
    let context = change_context(fixture.owner.clone(), 50, '5');
    // Both preparations miss: neither request has committed yet.
    let first = providers
        .prepare_regeneration(&context, request.clone())
        .await
        .expect("prepare first");
    let second = providers
        .prepare_regeneration(&context, request.clone())
        .await
        .expect("prepare second");
    let committed = providers
        .commit_publication(&context, first)
        .await
        .expect("commit first");
    let events = fixture.events(&providers).await;

    let replayed = providers
        .commit_publication(&context, second)
        .await
        .expect("commit second");
    assert_eq!(replayed.search, SearchChange::Unchanged);
    assert_eq!(replayed.value, committed.value);
    assert_eq!(
        fixture.events(&providers).await,
        events,
        "a replay writes no evidence"
    );

    let crate::publication::PreparedPublication::Replayed(stored) = providers
        .prepare_regeneration(&context, request)
        .await
        .expect("prepare after commit")
    else {
        panic!("preparation replays a committed request");
    };
    assert_eq!(stored, committed.value);
}
