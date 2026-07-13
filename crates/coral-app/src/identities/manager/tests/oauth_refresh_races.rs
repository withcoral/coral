use super::*;

const REFRESH_RACE_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
async fn sqlite_oauth_refresh_race_contract() {
    let temp = tempdir().expect("temp dir");
    let db = Arc::new(
        CoralDb::open(ResolvedDatabaseConfig::Sqlite {
            path: temp.path().join("coral.sqlite"),
        })
        .await
        .expect("open sqlite"),
    );
    db.migrate().await.expect("migrate sqlite");
    Box::pin(assert_oauth_refresh_race_contract(&db)).await;
}

pub(crate) async fn assert_oauth_refresh_race_contract(db: &Arc<CoralDb>) {
    Box::pin(assert_same_identity_converges(db)).await;
    Box::pin(assert_replacement_wins(db, ReplacementKind::FixedToken)).await;
    Box::pin(assert_replacement_wins(db, ReplacementKind::OAuth)).await;
    Box::pin(assert_delete_recreate_aba_wins(db)).await;
    Box::pin(assert_deletion_wins(db)).await;
}

#[expect(clippy::used_underscore_binding, reason = "opaque revision contract")]
async fn assert_same_identity_converges(db: &Arc<CoralDb>) {
    let fixture = create_refresh_manager_fixture(db, false).await;
    let before_document = load_pair(db, &fixture.owner, fixture.name.as_str())
        .await
        .1
        .expect("identity document before refresh");
    let first_claim_reached = Arc::new(tokio::sync::Barrier::new(2));
    let first_claim_resume = Arc::new(tokio::sync::Barrier::new(2));
    let second_claim_reached = Arc::new(tokio::sync::Barrier::new(2));
    let second_claim_resume = Arc::new(tokio::sync::Barrier::new(2));
    let finalize_reached = Arc::new(tokio::sync::Barrier::new(2));
    let finalize_resume = Arc::new(tokio::sync::Barrier::new(2));
    let wait_reached = Arc::new(tokio::sync::Barrier::new(2));
    let wait_resume = Arc::new(tokio::sync::Barrier::new(2));
    let first_manager = refresh_manager(db, &fixture.keys)
        .with_before_refresh_claim_gate(first_claim_reached.clone(), first_claim_resume.clone())
        .with_before_refresh_finalize_gate(finalize_reached.clone(), finalize_resume.clone());
    let second_manager = refresh_manager(db, &fixture.keys)
        .with_before_refresh_claim_gate(second_claim_reached.clone(), second_claim_resume.clone())
        .with_before_refresh_wait_gate(wait_reached.clone(), wait_resume.clone());
    let first_owner = fixture.owner.clone();
    let first_name = fixture.name.as_str().to_string();
    let second_owner = fixture.owner.clone();
    let second_name = first_name.clone();

    let (first, second) = tokio::time::timeout(REFRESH_RACE_TIMEOUT, async move {
        let first =
            tokio::spawn(async move { first_manager.get_for_use(&first_owner, &first_name).await });
        first_claim_reached.wait().await;
        let second = tokio::spawn(async move {
            second_manager
                .get_for_use(&second_owner, &second_name)
                .await
        });
        second_claim_reached.wait().await;
        first_claim_resume.wait().await;
        finalize_reached.wait().await;
        second_claim_resume.wait().await;
        wait_reached.wait().await;
        finalize_resume.wait().await;
        let first = first.await.expect("first refresh task");
        wait_resume.wait().await;
        let second = second.await.expect("waiting refresh task");
        (first, second)
    })
    .await
    .expect("same-identity refresh race must not deadlock");
    let first = first.expect("claimant refresh succeeds");
    let second = second.expect("waiter reloads committed refresh");
    assert_oauth_token(&first, "refreshed-token");
    assert_oauth_token(&second, "refreshed-token");

    let committed = load_use_snapshot(db, &fixture.owner, &fixture.name).await;
    assert!(committed.oauth_refresh_claim.is_none());
    assert!(first.revision()._snapshot == committed);
    assert!(second.revision()._snapshot == committed);
    let after_document = committed
        .identity_document
        .as_ref()
        .expect("committed refresh document");
    assert_eq!(
        after_document.document_version,
        before_document.document_version + 1
    );
    assert_eq!(refresh_request_count(&fixture.provider).await, 1);
}

#[derive(Clone, Copy, Debug)]
enum ReplacementKind {
    FixedToken,
    OAuth,
}

async fn assert_replacement_wins(db: &Arc<CoralDb>, kind: ReplacementKind) {
    let fixture = create_refresh_manager_fixture(db, false).await;
    let principal = fixture_principal(&fixture);
    let original_spec = fixture_spec_name(db, &fixture).await;
    let before_document = load_pair(db, &fixture.owner, fixture.name.as_str())
        .await
        .1
        .unwrap();
    let fixed_spec = format!("refresh_replacement_{}", uuid::Uuid::new_v4().simple());
    if matches!(kind, ReplacementKind::FixedToken) {
        put_spec(
            db,
            &IdentitySpecKey::global(&fixed_spec).unwrap(),
            &fixed_manifest(&fixed_spec, "refresh_replacement"),
        )
        .await;
    }
    let replacement_manager = fixture.manager.clone();
    let refresh_manager = refresh_manager(db, &fixture.keys);
    let (refresh, replacement) = race_at_refresh_finalize(
        refresh_manager,
        &fixture.owner,
        fixture.name.as_str(),
        || async {
            match kind {
                ReplacementKind::FixedToken => {
                    replacement_manager
                        .create_or_replace_user_fixed_token(
                            &principal,
                            fixture.name.as_str(),
                            &fixed_spec,
                            "replacement-token".into(),
                        )
                        .await
                }
                ReplacementKind::OAuth => {
                    replacement_manager
                        .create_or_replace_user_oauth(
                            &principal,
                            fixture.name.as_str(),
                            &original_spec,
                            IdentityOAuthCommitPhase::default(),
                            |_event| async { Ok(()) },
                        )
                        .await
                }
            }
            .expect("explicit replacement wins refresh race")
        },
    )
    .await;
    assert_refresh_reconnect(refresh);

    let (Some(record), Some(document)) = load_pair(db, &fixture.owner, fixture.name.as_str()).await
    else {
        panic!("replacement pair must remain durable");
    };
    assert_eq!(record, replacement);
    assert_eq!(
        document.document_version,
        before_document.document_version + 1
    );
    assert!(
        load_use_snapshot(db, &fixture.owner, &fixture.name)
            .await
            .oauth_refresh_claim
            .is_none()
    );
    match kind {
        ReplacementKind::FixedToken => assert_material(
            &record,
            &document,
            "replacement-token",
            fixture.keys.as_ref(),
        ),
        ReplacementKind::OAuth => {
            let material = decrypt_material(&record, &document, fixture.keys.as_ref());
            assert_eq!(
                material.get("ACCESS_TOKEN").map(String::as_str),
                Some("access-token")
            );
            assert!(material.values().all(|value| value != "refreshed-token"));
        }
    }
    assert_eq!(refresh_request_count(&fixture.provider).await, 1);
}

async fn assert_delete_recreate_aba_wins(db: &Arc<CoralDb>) {
    let fixture = create_refresh_manager_fixture(db, false).await;
    let principal = fixture_principal(&fixture);
    let spec_name = fixture_spec_name(db, &fixture).await;
    let (Some(before_record), Some(before_document)) =
        load_pair(db, &fixture.owner, fixture.name.as_str()).await
    else {
        panic!("identity pair before delete/recreate race");
    };
    let mutation_manager = fixture.manager.clone();
    let refresh_manager = refresh_manager(db, &fixture.keys);
    let (refresh, recreated) = race_at_refresh_finalize(
        refresh_manager,
        &fixture.owner,
        fixture.name.as_str(),
        || async {
            mutation_manager
                .delete(&fixture.owner, fixture.name.as_str())
                .await
                .expect("delete claimed identity");
            mutation_manager
                .create_or_replace_user_oauth(
                    &principal,
                    fixture.name.as_str(),
                    &spec_name,
                    IdentityOAuthCommitPhase::default(),
                    |_event| async { Ok(()) },
                )
                .await
                .expect("recreate exact OAuth identity generation")
        },
    )
    .await;
    assert_refresh_reconnect(refresh);

    let (Some(record), Some(document)) = load_pair(db, &fixture.owner, fixture.name.as_str()).await
    else {
        panic!("recreated identity pair must remain durable");
    };
    assert_eq!(record, recreated);
    assert_eq!(record.spec_reference, before_record.spec_reference);
    assert_eq!(before_document.document_version, 1);
    assert_eq!(document.document_version, 1);
    assert_ne!(document, before_document);
    let material = decrypt_material(&record, &document, fixture.keys.as_ref());
    assert_eq!(
        material.get("ACCESS_TOKEN").map(String::as_str),
        Some("access-token")
    );
    assert!(material.values().all(|value| value != "refreshed-token"));
    assert!(
        load_use_snapshot(db, &fixture.owner, &fixture.name)
            .await
            .oauth_refresh_claim
            .is_none()
    );
    assert_eq!(refresh_request_count(&fixture.provider).await, 1);
}

async fn assert_deletion_wins(db: &Arc<CoralDb>) {
    let fixture = create_refresh_manager_fixture(db, false).await;
    let mutation_manager = fixture.manager.clone();
    let refresh_manager = refresh_manager(db, &fixture.keys);
    let (refresh, ()) = race_at_refresh_finalize(
        refresh_manager,
        &fixture.owner,
        fixture.name.as_str(),
        || async {
            mutation_manager
                .delete(&fixture.owner, fixture.name.as_str())
                .await
                .expect("delete claimed identity");
        },
    )
    .await;
    assert_refresh_reconnect(refresh);
    assert_eq!(
        load_pair(db, &fixture.owner, fixture.name.as_str()).await,
        (None, None)
    );
    let snapshot = load_use_snapshot(db, &fixture.owner, &fixture.name).await;
    assert!(snapshot.identity.is_none());
    assert!(snapshot.identity_document.is_none());
    assert!(snapshot.oauth_refresh_claim.is_none());
    assert_eq!(refresh_request_count(&fixture.provider).await, 1);
}

async fn race_at_refresh_finalize<T, F, Fut>(
    manager: IdentityManager,
    owner: &IdentityOwner,
    name: &str,
    mutate: F,
) -> (Result<ResolvedIdentityForUse, AppError>, T)
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    let reached = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let gated = manager.with_before_refresh_finalize_gate(reached.clone(), resume.clone());
    tokio::time::timeout(REFRESH_RACE_TIMEOUT, async {
        tokio::join!(gated.get_for_use(owner, name), async {
            reached.wait().await;
            let result = mutate().await;
            resume.wait().await;
            result
        })
    })
    .await
    .expect("OAuth refresh finalization race must not deadlock")
}

fn refresh_manager(db: &Arc<CoralDb>, keys: &Arc<TestKeyProvider>) -> IdentityManager {
    IdentityManager::new(db.clone(), keys.clone())
}

fn fixture_principal(fixture: &RefreshManagerFixture) -> UserPrincipal {
    match &fixture.owner {
        IdentityOwner::User(principal) => principal.clone(),
        IdentityOwner::Workspace(_) => panic!("refresh fixture must be user-owned"),
    }
}

async fn fixture_spec_name(db: &Arc<CoralDb>, fixture: &RefreshManagerFixture) -> String {
    load_pair(db, &fixture.owner, fixture.name.as_str())
        .await
        .0
        .expect("refresh fixture identity")
        .spec_reference
        .key()
        .name()
        .to_string()
}

fn assert_oauth_token(resolved: &ResolvedIdentityForUse, expected: &str) {
    assert_eq!(
        resolved.material().get("ACCESS_TOKEN").map(String::as_str),
        Some(expected)
    );
}

fn assert_refresh_reconnect(result: Result<ResolvedIdentityForUse, AppError>) {
    let Err(AppError::FailedPrecondition(detail)) = result else {
        panic!("stale claimed refresh must fail closed");
    };
    assert!(detail.contains("reconnect the identity"));
    for secret in ["access-token", "refresh-token", "refreshed-token"] {
        assert!(
            !detail.contains(secret),
            "refresh diagnostic leaked {secret}"
        );
    }
}

async fn refresh_request_count(provider: &MockServer) -> usize {
    refresh_request_bodies(provider).await.len()
}

async fn refresh_request_bodies(provider: &MockServer) -> Vec<String> {
    provider
        .received_requests()
        .await
        .expect("recorded OAuth provider requests")
        .into_iter()
        .filter(|request| {
            String::from_utf8_lossy(&request.body).contains("grant_type=refresh_token")
        })
        .map(|request| String::from_utf8_lossy(&request.body).into_owned())
        .collect()
}
