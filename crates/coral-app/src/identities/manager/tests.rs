use std::sync::Arc;
use std::time::Duration;

use coral_spec::parse_identity_manifest_yaml;

use super::IdentityManager;
use crate::bootstrap::AppError;
use crate::credentials::CredentialsError;
use crate::credentials::encryption::{CredentialEncryptionKey, CredentialKeyProvider};
use crate::identities::crypto::{IdentityDocumentBinding, decrypt_identity_document};
use crate::identities::model::{IdentityName, IdentityOwner};
use crate::identity::{Principal, PrincipalKind};
use crate::identity_specs::identity_spec_fingerprint;
use crate::state::db::{
    CoralDb, DbRepos, IdentityDocumentRecord, IdentityRecord, IdentitySpecKey,
    set_identity_document_version,
};
use crate::workspaces::WorkspaceName;

struct TestKeyProvider(Vec<CredentialEncryptionKey>);

impl CredentialKeyProvider for TestKeyProvider {
    fn active_key(&self) -> Result<CredentialEncryptionKey, CredentialsError> {
        self.0
            .last()
            .cloned()
            .ok_or_else(|| CredentialsError::Unavailable("missing test key".to_string()))
    }

    fn key(&self, key_id: &str) -> Result<CredentialEncryptionKey, CredentialsError> {
        self.0
            .iter()
            .find(|key| key.key_id() == key_id)
            .cloned()
            .ok_or_else(|| CredentialsError::Unavailable("missing test key".to_string()))
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "shared SQLite/Postgres manager contract"
)]
pub(crate) async fn assert_user_global_fixed_token_create_contract(db: &Arc<CoralDb>) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fixed_a = format!("fixeda_{suffix}");
    let fixed_b = format!("fixedb_{suffix}");
    let oauth = format!("oauth_{suffix}");
    let race = format!("race_{suffix}");
    let missing = format!("missing_{suffix}");
    for (name, yaml) in [
        (&fixed_a, fixed_manifest(&fixed_a, "a")),
        (&fixed_b, fixed_manifest(&fixed_b, "b")),
        (&oauth, oauth_manifest(&oauth)),
        (&race, fixed_manifest(&race, "before")),
    ] {
        put_global_spec(db, name, &yaml).await;
    }

    let principal =
        Principal::parse(&format!("user-{suffix}"), PrincipalKind::User).expect("principal");
    let owner = IdentityOwner::for_user(principal.clone());
    let unavailable = Arc::new(TestKeyProvider(Vec::new()));
    let unavailable_manager = IdentityManager::new(db.clone(), unavailable);
    for (name, spec, token, expected) in [
        ("blank", fixed_a.as_str(), " \t ", "invalid"),
        ("missing", missing.as_str(), "token", "missing"),
        ("oauth", oauth.as_str(), "token", "invalid"),
        ("no_key", fixed_a.as_str(), "token", "credentials"),
    ] {
        let identity_name = format!("{name}_{suffix}");
        let error = unavailable_manager
            .create_or_replace_user_fixed_token(&principal, &identity_name, spec, token.to_string())
            .await
            .expect_err("invalid creation must fail");
        match expected {
            "missing" => assert!(matches!(
                error,
                AppError::IdentitySpecNotFound { scope, .. } if scope == "global"
            )),
            "credentials" => assert!(matches!(error, AppError::Credentials(_))),
            _ => assert!(matches!(error, AppError::InvalidInput(_))),
        }
        assert_eq!(load_pair(db, &owner, &identity_name).await, (None, None));
    }

    let old_key = CredentialEncryptionKey::from_static_bytes_for_test([61; 32]);
    let old_provider = Arc::new(TestKeyProvider(vec![old_key.clone()]));
    let manager = IdentityManager::new(db.clone(), old_provider.clone());
    let identity = format!("primary_{suffix}");
    let created = manager
        .create_or_replace_user_fixed_token(
            &principal,
            &identity,
            &fixed_a,
            "  alpha-token  ".to_string(),
        )
        .await
        .expect("create fixed-token identity");
    assert_reference(&created, &fixed_a, "a");
    let created_pair = load_pair(db, &owner, &identity).await;
    assert_eq!(created_pair.0.as_ref(), Some(&created));
    let created_document = created_pair.1.as_ref().expect("created document");
    assert_eq!(created_document.document_version, 1);
    assert_eq!(created_document.envelope.key_id, old_key.key_id());
    assert_material(
        &created,
        created_document,
        "alpha-token",
        old_provider.as_ref(),
    );

    let new_key = CredentialEncryptionKey::from_static_bytes_for_test([62; 32]);
    let rotated_provider = Arc::new(TestKeyProvider(vec![old_key, new_key.clone()]));
    let rotated = IdentityManager::new(db.clone(), rotated_provider.clone());
    let replaced = rotated
        .create_or_replace_user_fixed_token(
            &principal,
            &identity,
            &fixed_b,
            " beta-token ".to_string(),
        )
        .await
        .expect("replace fixed-token identity");
    assert_reference(&replaced, &fixed_b, "b");
    let replaced_pair = load_pair(db, &owner, &identity).await;
    let replaced_document = replaced_pair.1.as_ref().expect("replaced document");
    assert_eq!(
        replaced.created_at_unix_nanos,
        created.created_at_unix_nanos
    );
    assert_eq!(replaced_document.document_version, 2);
    assert_eq!(
        replaced_document.created_at_unix_nanos,
        created_document.created_at_unix_nanos
    );
    assert_eq!(replaced_document.envelope.key_id, new_key.key_id());
    assert_material(
        &replaced,
        replaced_document,
        "beta-token",
        rotated_provider.as_ref(),
    );

    let identity_name = IdentityName::parse(&identity).expect("identity name");
    set_identity_document_version(db, &owner, &identity_name, i64::MAX).await;
    let before_failure = load_pair(db, &owner, &identity).await;
    let error = rotated
        .create_or_replace_user_fixed_token(
            &principal,
            &identity,
            &fixed_a,
            "rollback-token".to_string(),
        )
        .await
        .expect_err("exhausted document version must fail");
    assert!(
        matches!(error, AppError::FailedPrecondition(detail) if detail.contains("version is exhausted"))
    );
    assert_eq!(load_pair(db, &owner, &identity).await, before_failure);

    let conflict_name = format!("conflict_{suffix}");
    let conflict_gate = Arc::new(tokio::sync::Barrier::new(2));
    let left = rotated
        .clone()
        .with_before_upsert_gate(conflict_gate.clone());
    let right = rotated.clone().with_before_upsert_gate(conflict_gate);
    let (left, right) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            left.create_or_replace_user_fixed_token(
                &principal,
                &conflict_name,
                &fixed_a,
                "left-token".to_string(),
            ),
            right.create_or_replace_user_fixed_token(
                &principal,
                &conflict_name,
                &fixed_b,
                "right-token".to_string(),
            ),
        )
    })
    .await
    .expect("conflicting writes must not deadlock");
    let left = left.expect("left write");
    let right = right.expect("right write");
    assert_reference(&left, &fixed_a, "a");
    assert_reference(&right, &fixed_b, "b");
    let conflict = load_pair(db, &owner, &conflict_name).await;
    let conflict_identity = conflict.0.as_ref().expect("conflict identity");
    let conflict_document = conflict.1.as_ref().expect("conflict document");
    assert_eq!(conflict_document.document_version, 2);
    let (winning, winning_token) = match conflict_identity.spec_reference.key().name() {
        name if name == fixed_a.as_str() => (&left, "left-token"),
        name if name == fixed_b.as_str() => (&right, "right-token"),
        name => panic!("unexpected winning spec {name}"),
    };
    assert_eq!(conflict_identity, winning);
    assert_material(
        conflict_identity,
        conflict_document,
        winning_token,
        rotated_provider.as_ref(),
    );

    let prepared = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let gated = rotated
        .clone()
        .with_before_write_gate(prepared.clone(), resume.clone());
    let raced_name = format!("raced_{suffix}");
    let raced = tokio::time::timeout(Duration::from_secs(10), async {
        let create = gated.create_or_replace_user_fixed_token(
            &principal,
            &raced_name,
            &race,
            "race-token".to_string(),
        );
        let replace = async {
            prepared.wait().await;
            put_global_spec(db, &race, &fixed_manifest(&race, "after")).await;
            resume.wait().await;
        };
        tokio::join!(create, replace).0
    })
    .await
    .expect("spec replacement race must not deadlock")
    .expect("raced create");
    assert_reference(&raced, &race, "after");
    let raced_pair = load_pair(db, &owner, &raced_name).await;
    assert_eq!(raced_pair.0.as_ref(), Some(&raced));
    assert_eq!(
        raced_pair
            .1
            .as_ref()
            .expect("raced document")
            .document_version,
        1
    );
    assert_material(
        &raced,
        raced_pair.1.as_ref().expect("raced document"),
        "race-token",
        rotated_provider.as_ref(),
    );

    let mut cleanup = db.begin().await.expect("begin identity cleanup");
    let conflict_identity_name = IdentityName::parse(&conflict_name).expect("conflict name");
    let raced_identity_name = IdentityName::parse(&raced_name).expect("raced name");
    for name in [
        &identity_name,
        &conflict_identity_name,
        &raced_identity_name,
    ] {
        assert!(
            cleanup
                .identities()
                .delete(&owner, name)
                .await
                .expect("delete test identity")
        );
    }
    for spec_name in [&fixed_a, &fixed_b, &oauth, &race] {
        assert!(
            cleanup
                .identity_specs()
                .delete(&IdentitySpecKey::global(spec_name).expect("cleanup spec key"))
                .await
                .expect("delete test identity spec")
        );
    }
    cleanup.commit().await.expect("commit identity cleanup");
}

#[expect(
    clippy::too_many_lines,
    reason = "shared SQLite/Postgres workspace manager contract"
)]
pub(crate) async fn assert_workspace_fixed_token_create_contract(db: &Arc<CoralDb>) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let workspace = WorkspaceName::parse(&format!("workspace{suffix}")).expect("workspace");
    let other = WorkspaceName::parse(&format!("other{suffix}")).expect("other workspace");
    let recreated = WorkspaceName::parse(&format!("recreated{suffix}")).expect("recreated");
    let missing_workspace =
        WorkspaceName::parse(&format!("missing{suffix}")).expect("missing workspace");
    put_workspace(db, &workspace).await;
    put_workspace(db, &other).await;
    put_workspace(db, &recreated).await;

    let fallback = format!("fallback_{suffix}");
    let shadowed = format!("shadowed_{suffix}");
    let wrong_type = format!("wrong_{suffix}");
    let generation = format!("generation_{suffix}");
    let missing_spec = format!("missing_spec_{suffix}");
    let fallback_global = IdentitySpecKey::global(&fallback).expect("fallback global key");
    let fallback_workspace =
        IdentitySpecKey::workspace(workspace.clone(), &fallback).expect("fallback workspace key");
    let fallback_other =
        IdentitySpecKey::workspace(other.clone(), &fallback).expect("other fallback key");
    let shadowed_global = IdentitySpecKey::global(&shadowed).expect("shadowed global key");
    let shadowed_workspace =
        IdentitySpecKey::workspace(workspace.clone(), &shadowed).expect("shadowed workspace key");
    let wrong_global = IdentitySpecKey::global(&wrong_type).expect("wrong global key");
    let wrong_workspace =
        IdentitySpecKey::workspace(workspace.clone(), &wrong_type).expect("wrong workspace key");
    let generation_global = IdentitySpecKey::global(&generation).expect("generation global key");
    for (key, yaml) in [
        (
            &fallback_global,
            fixed_manifest(&fallback, "fallback_global"),
        ),
        (
            &fallback_other,
            fixed_manifest(&fallback, "other_workspace"),
        ),
        (
            &shadowed_global,
            fixed_manifest(&shadowed, "shadowed_global"),
        ),
        (
            &shadowed_workspace,
            fixed_manifest(&shadowed, "shadowed_workspace"),
        ),
        (&wrong_global, fixed_manifest(&wrong_type, "wrong_global")),
        (&wrong_workspace, oauth_manifest(&wrong_type)),
        (
            &generation_global,
            fixed_manifest(&generation, "generation"),
        ),
    ] {
        put_spec(db, key, &yaml).await;
    }

    let old_key = CredentialEncryptionKey::from_static_bytes_for_test([73; 32]);
    let old_provider = Arc::new(TestKeyProvider(vec![old_key.clone()]));
    let manager = IdentityManager::new(db.clone(), old_provider.clone());
    let unavailable = IdentityManager::new(db.clone(), Arc::new(TestKeyProvider(Vec::new())));
    let owner = IdentityOwner::workspace(workspace.clone());

    let missing_workspace_identity = format!("missing_workspace_{suffix}");
    assert!(matches!(
        manager
            .create_or_replace_workspace_fixed_token(
                &missing_workspace,
                &missing_workspace_identity,
                &fallback,
                "token".to_string(),
            )
            .await,
        Err(AppError::WorkspaceNotFound(name)) if name == missing_workspace.as_str()
    ));
    let missing_spec_identity = format!("missing_spec_{suffix}");
    assert!(matches!(
        manager
            .create_or_replace_workspace_fixed_token(
                &workspace,
                &missing_spec_identity,
                &missing_spec,
                "token".to_string(),
            )
            .await,
        Err(AppError::IdentitySpecNotFound { scope, .. })
            if scope == format!("workspace:{workspace}")
    ));
    let wrong_type_identity = format!("wrong_type_{suffix}");
    assert!(matches!(
        manager
            .create_or_replace_workspace_fixed_token(
                &workspace,
                &wrong_type_identity,
                &wrong_type,
                "token".to_string(),
            )
            .await,
        Err(AppError::InvalidInput(_))
    ));
    let blank_identity = format!("blank_{suffix}");
    assert!(matches!(
        manager
            .create_or_replace_workspace_fixed_token(
                &workspace,
                &blank_identity,
                &fallback,
                " \t ".to_string(),
            )
            .await,
        Err(AppError::InvalidInput(_))
    ));
    let no_key_identity = format!("no_key_{suffix}");
    assert!(matches!(
        unavailable
            .create_or_replace_workspace_fixed_token(
                &workspace,
                &no_key_identity,
                &fallback,
                "token".to_string(),
            )
            .await,
        Err(AppError::Credentials(_))
    ));
    for name in [
        &missing_spec_identity,
        &wrong_type_identity,
        &blank_identity,
        &no_key_identity,
    ] {
        assert_eq!(load_pair(db, &owner, name).await, (None, None));
    }

    let fallback_identity = format!("fallback_identity_{suffix}");
    let fallback_created = manager
        .create_or_replace_workspace_fixed_token(
            &workspace,
            &fallback_identity,
            &fallback,
            " fallback-token ".to_string(),
        )
        .await
        .expect("create from global fallback");
    assert_reference_key(&fallback_created, &fallback_global, "fallback_global");
    let fallback_created_pair = load_pair(db, &owner, &fallback_identity).await;
    let fallback_created_document = fallback_created_pair.1.as_ref().expect("fallback document");
    assert_eq!(fallback_created_pair.0.as_ref(), Some(&fallback_created));
    assert_eq!(fallback_created_document.document_version, 1);
    assert_eq!(fallback_created_document.envelope.key_id, old_key.key_id());
    assert_material(
        &fallback_created,
        fallback_created_document,
        "fallback-token",
        old_provider.as_ref(),
    );

    let shadowed_identity = format!("shadowed_identity_{suffix}");
    let shadowed_created = manager
        .create_or_replace_workspace_fixed_token(
            &workspace,
            &shadowed_identity,
            &shadowed,
            "workspace-token".to_string(),
        )
        .await
        .expect("create from workspace override");
    assert_reference_key(&shadowed_created, &shadowed_workspace, "shadowed_workspace");
    let shadowed_pair = load_pair(db, &owner, &shadowed_identity).await;
    assert_eq!(shadowed_pair.0.as_ref(), Some(&shadowed_created));
    assert_eq!(
        shadowed_pair
            .1
            .as_ref()
            .expect("shadowed document")
            .document_version,
        1
    );
    assert_material(
        &shadowed_created,
        shadowed_pair.1.as_ref().expect("shadowed document"),
        "workspace-token",
        old_provider.as_ref(),
    );

    put_spec(
        db,
        &fallback_workspace,
        &fixed_manifest(&fallback, "late_workspace"),
    )
    .await;
    let new_key = CredentialEncryptionKey::from_static_bytes_for_test([74; 32]);
    let rotated_provider = Arc::new(TestKeyProvider(vec![old_key, new_key.clone()]));
    let rotated = IdentityManager::new(db.clone(), rotated_provider.clone());
    let fallback_replaced = rotated
        .create_or_replace_workspace_fixed_token(
            &workspace,
            &fallback_identity,
            &fallback,
            "replacement-token".to_string(),
        )
        .await
        .expect("replace from late workspace override");
    assert_reference_key(&fallback_replaced, &fallback_workspace, "late_workspace");
    let fallback_replaced_pair = load_pair(db, &owner, &fallback_identity).await;
    assert_eq!(fallback_replaced_pair.0.as_ref(), Some(&fallback_replaced));
    let fallback_replaced_document = fallback_replaced_pair
        .1
        .as_ref()
        .expect("replaced document");
    assert_eq!(
        fallback_replaced.created_at_unix_nanos,
        fallback_created.created_at_unix_nanos
    );
    assert_eq!(fallback_replaced_document.document_version, 2);
    assert_eq!(
        fallback_replaced_document.created_at_unix_nanos,
        fallback_created_document.created_at_unix_nanos
    );
    assert_eq!(fallback_replaced_document.envelope.key_id, new_key.key_id());
    assert_material(
        &fallback_replaced,
        fallback_replaced_document,
        "replacement-token",
        rotated_provider.as_ref(),
    );

    let fallback_name = IdentityName::parse(&fallback_identity).expect("fallback identity name");
    set_identity_document_version(db, &owner, &fallback_name, i64::MAX).await;
    let before_failure = load_pair(db, &owner, &fallback_identity).await;
    let error = rotated
        .create_or_replace_workspace_fixed_token(
            &workspace,
            &fallback_identity,
            &shadowed,
            "rollback-token".to_string(),
        )
        .await
        .expect_err("exhausted document version must fail");
    assert!(
        matches!(error, AppError::FailedPrecondition(detail) if detail.contains("version is exhausted"))
    );
    assert_eq!(
        load_pair(db, &owner, &fallback_identity).await,
        before_failure
    );

    let prepared = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let gated = rotated
        .clone()
        .with_before_write_gate(prepared.clone(), resume.clone());
    let generation_identity = format!("generation_identity_{suffix}");
    let generation_result = tokio::time::timeout(Duration::from_secs(10), async {
        let create = gated.create_or_replace_workspace_fixed_token(
            &recreated,
            &generation_identity,
            &generation,
            "generation-token".to_string(),
        );
        let recreate = async {
            prepared.wait().await;
            let mut tx = db.begin().await.expect("begin workspace recreation");
            tx.workspaces()
                .delete(recreated.as_str())
                .await
                .expect("delete workspace generation");
            tx.workspaces()
                .ensure(recreated.as_str(), 2)
                .await
                .expect("recreate workspace generation");
            tx.commit().await.expect("commit workspace recreation");
            resume.wait().await;
        };
        tokio::join!(create, recreate).0
    })
    .await
    .expect("workspace recreation race must not deadlock");
    assert!(matches!(
        generation_result,
        Err(AppError::WorkspaceNotFound(name)) if name == recreated.as_str()
    ));
    let recreated_owner = IdentityOwner::workspace(recreated.clone());
    assert_eq!(
        load_pair(db, &recreated_owner, &generation_identity).await,
        (None, None)
    );

    let mut cleanup = db.begin().await.expect("begin workspace cleanup");
    let shadowed_name = IdentityName::parse(&shadowed_identity).expect("shadowed identity name");
    for name in [&fallback_name, &shadowed_name] {
        assert!(cleanup.identities().delete(&owner, name).await.unwrap());
    }
    for key in [
        &fallback_global,
        &fallback_workspace,
        &fallback_other,
        &shadowed_global,
        &shadowed_workspace,
        &wrong_global,
        &wrong_workspace,
        &generation_global,
    ] {
        assert!(cleanup.identity_specs().delete(key).await.unwrap());
    }
    cleanup
        .workspaces()
        .delete(workspace.as_str())
        .await
        .unwrap();
    cleanup.workspaces().delete(other.as_str()).await.unwrap();
    cleanup
        .workspaces()
        .delete(recreated.as_str())
        .await
        .unwrap();
    cleanup.commit().await.expect("commit workspace cleanup");
}

#[expect(
    clippy::too_many_lines,
    reason = "shared SQLite/Postgres workspace race contract"
)]
pub(crate) async fn assert_workspace_fixed_token_race_contract(db: &Arc<CoralDb>) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let shadow_workspace =
        WorkspaceName::parse(&format!("shadowrace{suffix}")).expect("shadow workspace");
    let generation_workspace =
        WorkspaceName::parse(&format!("generationrace{suffix}")).expect("generation workspace");
    put_workspace(db, &shadow_workspace).await;
    put_workspace(db, &generation_workspace).await;

    let shadow_spec = format!("shadow_race_{suffix}");
    let generation_spec = format!("generation_race_{suffix}");
    let shadow_global = IdentitySpecKey::global(&shadow_spec).expect("shadow global key");
    let shadow_workspace_key = IdentitySpecKey::workspace(shadow_workspace.clone(), &shadow_spec)
        .expect("shadow workspace key");
    let generation_global =
        IdentitySpecKey::global(&generation_spec).expect("generation global key");
    put_spec(db, &shadow_global, &fixed_manifest(&shadow_spec, "global")).await;
    put_spec(
        db,
        &generation_global,
        &fixed_manifest(&generation_spec, "before"),
    )
    .await;

    let provider = Arc::new(TestKeyProvider(vec![
        CredentialEncryptionKey::from_static_bytes_for_test([75; 32]),
    ]));
    let manager = IdentityManager::new(db.clone(), provider.clone());

    let shadow_prepared = Arc::new(tokio::sync::Barrier::new(2));
    let shadow_resume = Arc::new(tokio::sync::Barrier::new(2));
    let shadow_manager = manager
        .clone()
        .with_before_write_gate(shadow_prepared.clone(), shadow_resume.clone());
    let shadow_identity = format!("shadow_identity_{suffix}");
    let shadow_created = tokio::time::timeout(Duration::from_secs(10), async {
        let create = shadow_manager.create_or_replace_workspace_fixed_token(
            &shadow_workspace,
            &shadow_identity,
            &shadow_spec,
            "shadow-token".to_string(),
        );
        let install_shadow = async {
            shadow_prepared.wait().await;
            put_spec(
                db,
                &shadow_workspace_key,
                &fixed_manifest(&shadow_spec, "workspace"),
            )
            .await;
            shadow_resume.wait().await;
        };
        tokio::join!(create, install_shadow).0
    })
    .await
    .expect("workspace shadow race must not deadlock")
    .expect("create after workspace shadow");
    assert_reference_key(&shadow_created, &shadow_workspace_key, "workspace");
    let shadow_owner = IdentityOwner::workspace(shadow_workspace.clone());
    let shadow_pair = load_pair(db, &shadow_owner, &shadow_identity).await;
    assert_eq!(shadow_pair.0.as_ref(), Some(&shadow_created));
    let shadow_document = shadow_pair.1.as_ref().expect("shadow race document");
    assert_eq!(shadow_document.document_version, 1);
    assert_material(
        &shadow_created,
        shadow_document,
        "shadow-token",
        provider.as_ref(),
    );

    let generation_prepared = Arc::new(tokio::sync::Barrier::new(2));
    let generation_resume = Arc::new(tokio::sync::Barrier::new(2));
    let retry_prepared = Arc::new(tokio::sync::Barrier::new(2));
    let retry_resume = Arc::new(tokio::sync::Barrier::new(2));
    let generation_manager = manager
        .with_before_write_gate(generation_prepared.clone(), generation_resume.clone())
        .with_before_retry_gate(retry_prepared.clone(), retry_resume.clone());
    let generation_identity = format!("generation_identity_{suffix}");
    let generation_result = tokio::time::timeout(Duration::from_secs(10), async {
        let create = generation_manager.create_or_replace_workspace_fixed_token(
            &generation_workspace,
            &generation_identity,
            &generation_spec,
            "generation-token".to_string(),
        );
        let recreate = async {
            generation_prepared.wait().await;
            put_spec(
                db,
                &generation_global,
                &fixed_manifest(&generation_spec, "after"),
            )
            .await;
            generation_resume.wait().await;
            retry_prepared.wait().await;
            replace_workspace_generation(db, &generation_workspace, 2).await;
            retry_resume.wait().await;
        };
        tokio::join!(create, recreate).0
    })
    .await
    .expect("cross-attempt workspace recreation must not deadlock");
    assert!(matches!(
        generation_result,
        Err(AppError::WorkspaceNotFound(name)) if name == generation_workspace.as_str()
    ));
    let generation_owner = IdentityOwner::workspace(generation_workspace.clone());
    assert_eq!(
        load_pair(db, &generation_owner, &generation_identity).await,
        (None, None)
    );

    let mut cleanup = db.begin().await.expect("begin race cleanup");
    let shadow_name = IdentityName::parse(&shadow_identity).expect("shadow identity name");
    assert!(
        cleanup
            .identities()
            .delete(&shadow_owner, &shadow_name)
            .await
            .expect("delete shadow identity")
    );
    for key in [&shadow_global, &shadow_workspace_key, &generation_global] {
        assert!(
            cleanup
                .identity_specs()
                .delete(key)
                .await
                .expect("delete race spec")
        );
    }
    for workspace in [&shadow_workspace, &generation_workspace] {
        cleanup
            .workspaces()
            .delete(workspace.as_str())
            .await
            .expect("delete race workspace");
    }
    cleanup.commit().await.expect("commit race cleanup");
}

async fn put_workspace(db: &CoralDb, workspace: &WorkspaceName) {
    let mut tx = db.begin().await.expect("begin workspace write");
    tx.workspaces()
        .ensure(workspace.as_str(), 1)
        .await
        .expect("write workspace");
    tx.commit().await.expect("commit workspace write");
}

async fn replace_workspace_generation(
    db: &CoralDb,
    workspace: &WorkspaceName,
    created_at_unix_nanos: i64,
) {
    let mut tx = db.begin().await.expect("begin workspace replacement");
    tx.workspaces()
        .delete(workspace.as_str())
        .await
        .expect("delete workspace generation");
    tx.workspaces()
        .ensure(workspace.as_str(), created_at_unix_nanos)
        .await
        .expect("replace workspace generation");
    tx.commit().await.expect("commit workspace replacement");
}

async fn put_global_spec(db: &CoralDb, name: &str, yaml: &str) {
    let key = IdentitySpecKey::global(name).expect("global spec key");
    put_spec(db, &key, yaml).await;
}

async fn put_spec(db: &CoralDb, key: &IdentitySpecKey, yaml: &str) {
    let manifest = parse_identity_manifest_yaml(yaml).expect("identity manifest");
    let mut tx = db.begin().await.expect("begin spec write");
    tx.identity_specs()
        .upsert(key, &manifest, yaml, 1)
        .await
        .expect("upsert identity spec");
    tx.commit().await.expect("commit identity spec");
}

async fn load_pair(
    db: &CoralDb,
    owner: &IdentityOwner,
    identity_name: &str,
) -> (Option<IdentityRecord>, Option<IdentityDocumentRecord>) {
    let name = IdentityName::parse(identity_name).expect("identity name");
    let mut session = db;
    let identity = session
        .identities()
        .get(owner, &name)
        .await
        .expect("identity");
    let document = session
        .identity_documents()
        .get(owner, &name)
        .await
        .expect("identity document");
    (identity, document)
}

fn assert_reference(record: &IdentityRecord, spec_name: &str, revision: &str) {
    let key = IdentitySpecKey::global(spec_name).expect("global spec key");
    assert_reference_key(record, &key, revision);
}

fn assert_reference_key(record: &IdentityRecord, key: &IdentitySpecKey, revision: &str) {
    let manifest = parse_identity_manifest_yaml(&fixed_manifest(key.name(), revision))
        .expect("expected manifest");
    assert_eq!(record.spec_reference.key(), key);
    assert_eq!(
        record.spec_reference.fingerprint(),
        identity_spec_fingerprint(&manifest).expect("fingerprint")
    );
    assert_eq!(record.spec_reference.issuer(), format!("issuer_{revision}"));
    assert_eq!(record.spec_reference.identity_type(), "fixed_token");
}

fn assert_material(
    identity: &IdentityRecord,
    document: &IdentityDocumentRecord,
    token: &str,
    provider: &dyn CredentialKeyProvider,
) {
    let binding =
        IdentityDocumentBinding::new(&document.owner, &document.name, &identity.spec_reference)
            .expect("document binding");
    let kek = provider
        .key(&document.envelope.key_id)
        .expect("stored envelope key");
    let values =
        decrypt_identity_document(&binding, &document.envelope, &kek).expect("decrypt token");
    assert_eq!(values.get("TOKEN").map(String::as_str), Some(token));
    assert!(
        !document
            .envelope
            .ciphertext
            .windows(token.len())
            .any(|window| window == token.as_bytes())
    );
}

fn fixed_manifest(name: &str, revision: &str) -> String {
    format!(
        "kind: identity\nspec_version: 1\nname: {name}\nversion: {revision}\ndescription: {revision}\nissuer: issuer_{revision}\ntype: fixed_token\naudience: {{host: api.example.com}}\n"
    )
}

fn oauth_manifest(name: &str) -> String {
    format!(
        "kind: identity\nspec_version: 1\nname: {name}\nversion: oauth\ndescription: oauth\nissuer: oauth_issuer\ntype: oauth\naudience: {{host: api.example.com}}\noauth:\n  method:\n    flow: {{type: device_code}}\n    endpoints: {{device_authorization_url: 'https://provider.example.com/device', token_url: 'https://provider.example.com/token'}}\n    client: {{id: {{default: demo-client}}}}\n"
    )
}
