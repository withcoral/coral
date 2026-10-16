//! The terminal write reports what it committed beside the unchanged durable result.
use super::*;

use crate::{ActivatedGeneration, PreparedPublication, PublicationEvidence, Published};

/// Runs the caller's side of the split: one transaction around the write, then commit.
async fn write<T>(
    providers: &ProviderModule,
    context: &coral_workspace::ChangeContext,
    prepared: PreparedPublication<T>,
) -> Published<T> {
    let PreparedPublication::Materialized(publication) = prepared else {
        panic!("a first request has no committed result to replay");
    };
    let mut transaction = providers.database.begin().await.expect("begin");
    let published = providers
        .publish_in_transaction(&mut transaction, context, *publication)
        .await
        .expect("write the terminal result");
    transaction.commit().await.expect("commit");
    published
}

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
    let generation =
        crate::repository::provider_in_transaction(&mut transaction, workspace, provider)
            .await
            .expect("read Provider")
            .expect("Provider")
            .generation_id;
    transaction.rollback().await.expect("rollback");
    generation
}

#[tokio::test]
async fn creation_evidence_names_the_activated_generation_or_the_failed_outcome() {
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
    let failed = write(
        &providers,
        &context,
        providers
            .prepare_publication(&context, conflict)
            .await
            .expect("prepare"),
    )
    .await;
    assert_eq!(
        failed.publication.outcome(),
        ProviderPublicationOutcome::Failed
    );
    assert_eq!(failed.evidence, PublicationEvidence::Failed);

    let setup = fixture
        .replacement_setup(&providers, CREDENTIAL_FREE_OPENAPI, 60, '6')
        .await;
    let context = change_context(fixture.owner.clone(), 70, '7');
    let created = write(
        &providers,
        &context,
        providers
            .prepare_publication(
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
            .expect("prepare"),
    )
    .await;
    let provider = created
        .publication
        .published_provider()
        .expect("created Provider");
    let generation = active_generation(&providers, &fixture.workspace_id, provider.id()).await;
    assert_ne!(generation, active);
    assert_eq!(
        created.evidence,
        PublicationEvidence::Activated(ActivatedGeneration::new(
            fixture.workspace_id.clone(),
            provider.id().clone(),
            generation,
            None,
        ))
    );
}

#[tokio::test]
async fn regeneration_evidence_names_the_replaced_generation_the_reuse_or_the_failed_outcome() {
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
        let published = write(
            &providers,
            &context,
            providers
                .prepare_regeneration(
                    &context,
                    fixture.request(setup.id(), "15000000-0000-4000-8000-000000000012"),
                )
                .await
                .expect("prepare"),
        )
        .await;
        let after =
            active_generation(&providers, &fixture.workspace_id, &fixture.provider_id).await;
        let expected = match expected {
            Expected::Replaced => PublicationEvidence::Activated(ActivatedGeneration::new(
                fixture.workspace_id.clone(),
                fixture.provider_id.clone(),
                after,
                Some(before),
            )),
            Expected::Reused => PublicationEvidence::ReusedActiveGeneration,
            Expected::Failed => PublicationEvidence::Failed,
        };
        assert_eq!(published.evidence, expected, "{case}");
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
    let committed = write(&providers, &context, first).await;
    let events = fixture.events(&providers).await;

    let replayed = write(&providers, &context, second).await;
    assert_eq!(replayed.evidence, PublicationEvidence::Replayed);
    assert_eq!(replayed.publication, committed.publication);
    assert_eq!(
        fixture.events(&providers).await,
        events,
        "a replay writes no evidence"
    );

    let PreparedPublication::Replayed(stored) = providers
        .prepare_regeneration(&context, request)
        .await
        .expect("prepare after commit")
    else {
        panic!("preparation replays a committed request");
    };
    assert_eq!(stored, committed.publication);
}
