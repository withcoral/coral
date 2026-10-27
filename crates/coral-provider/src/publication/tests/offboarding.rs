//! Accepted Update work must not outlive the publisher's access.
mod agents;
mod fixture;

use super::*;
use crate::account::{self, AccountScope, AuthorizationState};
use crate::account_tests::{CREDENTIAL_REQUIRED_OPENAPI, authorization_request};
use crate::setup_tests::{credential_for, user_principal, workspace_owner};
use coral_administration::AccessLifecycleModule;
use coral_credentials::Secret;
use coral_workspace::{ChangeContext, OrganizationModule, WorkspaceRole};
use fixture::{assert_cleanup_events, candidate, evidence, harness, pause_update};

#[derive(Clone, Copy)]
enum Removal {
    Workspace,
    Organization,
}

#[tokio::test]
async fn workspace_member_removal_after_update_preflight_fences_publication_and_accounts() {
    for (initially_shared, shared) in [(false, false), (false, true), (true, true), (true, false)] {
        Box::pin(member_removal(Removal::Workspace, initially_shared, shared)).await;
    }
}

#[tokio::test]
async fn organization_member_removal_after_update_preflight_fences_publication_and_accounts() {
    for (initially_shared, shared) in [(false, false), (false, true), (true, true), (true, false)] {
        Box::pin(member_removal(
            Removal::Organization,
            initially_shared,
            shared,
        ))
        .await;
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "one admitted Update spans offboarding, terminal revalidation, and rejoining to prove all fences"
)]
async fn member_removal(removal: Removal, initially_shared: bool, shared: bool) {
    let h = harness().await;
    let (member, workspace) = workspace_owner(&h, "user:offboard-publisher").await;
    let owner =
        organization_user(h.database.clone(), "user:survivor", OrganizationRole::Owner).await;
    // A second designated Owner, so removing the publisher needs no transfer target.
    crate::tests::join_as_owner(&h.workspaces, &owner, workspace.id()).await;
    let context = change_context(member.clone(), 10, '1');
    let cleanup = change_context(owner.clone(), 30, '3');
    let provider = if initially_shared {
        h.providers
            .publish_shared_fixture(
                &context,
                workspace.id(),
                OpenApiDocument::parse(CREDENTIAL_REQUIRED_OPENAPI).unwrap(),
                "updated",
                &h.accounts,
                coral_credentials::CredentialValues::token(
                    Secret::new("initial-shared-secret".into()).unwrap(),
                ),
            )
            .await
            .unwrap()
    } else {
        publish_provider(
            &h.providers,
            &context,
            workspace.id(),
            CREDENTIAL_REQUIRED_OPENAPI,
            "updated",
            CredentialRoute::Caller,
        )
        .await
    };
    // A caller Account is cleaned up in every matrix case, including updates
    // whose target has a shared Account that must survive its provisioner.
    let private = if initially_shared {
        publish_provider(
            &h.providers,
            &context,
            workspace.id(),
            CREDENTIAL_REQUIRED_OPENAPI,
            "private",
            CredentialRoute::Caller,
        )
        .await
    } else {
        provider.clone()
    };
    let initial = h
        .accounts
        .start_user_authorization(
            &context,
            workspace.id(),
            private.id(),
            authorization_request(),
            None,
        )
        .await
        .unwrap();
    let connected = h
        .accounts
        .submit_secret(
            &context,
            workspace.id(),
            initial.id,
            coral_credentials::CredentialValues::token(
                Secret::new("caller-secret".into()).unwrap(),
            ),
        )
        .await
        .unwrap()
        .account_id
        .unwrap();
    let pending = h
        .accounts
        .start_reconnect(
            &context,
            workspace.id(),
            private.id(),
            connected,
            authorization_request(),
            None,
        )
        .await
        .unwrap();
    let retained = evidence(
        &h,
        workspace.id(),
        if initially_shared {
            "private"
        } else {
            "updated"
        },
        &user_principal(&member),
    )
    .await;
    assert_eq!(
        h.accounts
            .credentials(workspace.id(), private.id(), &retained, 11)
            .await
            .unwrap()
            .get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
        "caller-secret"
    );
    let update = candidate(&h, &context, workspace.id(), provider.id(), shared).await;
    let before = h
        .providers
        .inspect_provider(&owner, workspace.id(), provider.id())
        .await
        .unwrap();
    let tool = h
        .providers
        .inspect_tool(
            &owner,
            workspace.id(),
            provider.id(),
            &ToolKey::parse("getPrivateForecast").unwrap(),
        )
        .await
        .unwrap();
    let shared_before = if initially_shared {
        Some(
            h.accounts
                .get_account(
                    &cleanup,
                    workspace.id(),
                    provider.id(),
                    &AccountScope::Shared,
                )
                .await
                .unwrap(),
        )
    } else {
        None
    };
    let (gate, publishing) = pause_update(&h, &member, &update).await;
    let lifecycle = AccessLifecycleModule::new(h.database.clone(), h.workspaces.clone());
    tokio::time::timeout(Duration::from_secs(5), async {
        match removal {
            Removal::Workspace => {
                lifecycle
                    .remove_workspace_member(&cleanup, workspace.id(), member.user_id())
                    .await
            }
            Removal::Organization => {
                lifecycle
                    .remove_organization_member(&cleanup, member.user_id(), 1, None)
                    .await
            }
        }
        .unwrap();
    })
    .await
    .expect("offboarding commits while materialization is paused");
    assert!(!publishing.is_finished());
    h.accounts
        .credentials(workspace.id(), private.id(), &retained, 31)
        .await
        .unwrap_err();
    let events = h
        .providers
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    let canceled = events
        .iter()
        .filter(|event| {
            event.action() == LifecycleAction::SetupCanceled
                && event.target_id() == update.setup_id().to_string()
        })
        .collect::<Vec<_>>();
    assert_eq!(canceled.len(), 1);
    for event in canceled {
        assert_eq!(event.trace_id(), cleanup.trace_id());
        assert_eq!(event.occurred_at_unix_nanos(), 30);
    }
    match removal {
        Removal::Workspace => {
            let events = h
                .workspaces
                .list_lifecycle_events(&owner, workspace.id())
                .await
                .unwrap();
            let removed = events
                .iter()
                .filter(|event| {
                    event.action() == coral_workspace::LifecycleAction::MembershipRemoved
                })
                .collect::<Vec<_>>();
            assert_eq!(removed.len(), 1);
            for event in removed {
                assert_eq!(event.trace_id(), cleanup.trace_id());
                assert_eq!(event.occurred_at_unix_nanos(), 30);
            }
        }
        Removal::Organization => {
            let events = OrganizationModule::new(h.database.clone())
                .list_lifecycle_events(&owner)
                .await
                .unwrap();
            let removed = events
                .iter()
                .filter(|event| {
                    event.action == coral_workspace::OrganizationLifecycleAction::Removed
                })
                .collect::<Vec<_>>();
            assert_eq!(removed.len(), 1);
            for event in removed {
                assert_eq!(&event.trace_id, cleanup.trace_id());
                assert_eq!(event.occurred_at_unix_nanos, 30);
            }
        }
    }
    let private_events = h
        .accounts
        .list_user_lifecycle_events(&context, workspace.id())
        .await;
    assert!(
        private_events.is_err(),
        "removed caller has no event inspection authority"
    );
    assert_eq!(
        crate::tests::stored_setup(&h.database, workspace.id(), update.setup_id())
            .await
            .setup
            .state(),
        ProviderSetupState::Canceled
    );
    gate.release();
    let rejected = tokio::time::timeout(Duration::from_secs(5), publishing)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(matches!(
        rejected,
        crate::Error::Workspace(
            coral_workspace::Error::WorkspaceNotFound | coral_workspace::Error::PermissionDenied(_)
        )
    ));
    assert!(matches!(
        h.providers
            .get_regeneration_publication(&owner, workspace.id(), update.request_id())
            .await,
        Err(crate::Error::ProviderPublicationNotFound)
    ));
    assert_eq!(
        h.providers
            .inspect_provider(&owner, workspace.id(), provider.id())
            .await
            .unwrap(),
        before
    );
    assert_eq!(
        h.providers
            .inspect_tool(
                &owner,
                workspace.id(),
                provider.id(),
                &ToolKey::parse("getPrivateForecast").unwrap()
            )
            .await
            .unwrap(),
        tool
    );
    if let Some(shared_before) = shared_before {
        assert_eq!(
            h.accounts
                .get_account(
                    &cleanup,
                    workspace.id(),
                    provider.id(),
                    &AccountScope::Shared
                )
                .await
                .unwrap(),
            shared_before
        );
        assert_eq!(
            credential_for(
                &h,
                workspace.id(),
                provider.id(),
                "updated",
                "getPrivateForecast",
                &user_principal(&owner),
                31
            )
            .await
            .unwrap()
            .get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
            "initial-shared-secret"
        );
    }
    assert_eq!(
        h.providers
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .unwrap(),
        events
    );

    if matches!(removal, Removal::Organization) {
        OrganizationModule::new(h.database.clone())
            .provision_login(&context)
            .await
            .unwrap();
    }
    h.workspaces
        .set_membership(&cleanup, workspace.id(), &member, WorkspaceRole::Owner)
        .await
        .unwrap();
    let private_events = h
        .accounts
        .list_user_lifecycle_events(&context, workspace.id())
        .await
        .unwrap();
    assert_cleanup_events(&private_events, &cleanup, connected, pending.id);
    assert_eq!(
        h.accounts
            .submit_secret(
                &context,
                workspace.id(),
                pending.id,
                coral_credentials::CredentialValues::token(
                    Secret::new("late-secret".into()).unwrap()
                )
            )
            .await
            .unwrap()
            .state,
        AuthorizationState::Canceled
    );
    assert!(
        h.accounts
            .get_account(&context, workspace.id(), private.id(), &AccountScope::User)
            .await
            .unwrap()
            .account
            .is_none()
    );
    h.accounts
        .credentials(workspace.id(), private.id(), &retained, 32)
        .await
        .unwrap_err();
    h.providers
        .regenerate_provider(&context, update)
        .await
        .unwrap_err();
    assert_eq!(
        h.accounts
            .list_user_lifecycle_events(&context, workspace.id())
            .await
            .unwrap(),
        private_events
    );
    assert_eq!(
        h.providers
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .unwrap(),
        events
    );
}
