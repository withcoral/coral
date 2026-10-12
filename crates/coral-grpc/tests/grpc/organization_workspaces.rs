use super::*;

type Client =
    management::organization_workspace_service_client::OrganizationWorkspaceServiceClient<Channel>;

/// A second Workspace that only `member` belongs to, as its designated Owner.
/// The Organization Owner holds no Membership there.
async fn members_workspace(f: &Fixture) -> String {
    let workspace = f
        .workspaces
        .create(
            &change(&f.member),
            WorkspaceDisplayName::parse("Members only").unwrap(),
        )
        .await
        .unwrap();
    format!("workspaces/{}", workspace.id())
}

/// Reads one Workspace from the complete Organization listing, one page at a time.
async fn listed(client: &mut Client, token: &str, name: &str) -> management::OrganizationWorkspace {
    let mut page_token = String::new();
    loop {
        let page = client
            .list_organization_workspaces(request(
                management::ListOrganizationWorkspacesRequest {
                    page_size: 1,
                    page_token,
                },
                token,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(page.workspaces.len(), 1);
        if let Some(found) = page.workspaces.into_iter().find(|w| w.name == name) {
            return found;
        }
        page_token = page.next_page_token;
        assert!(!page_token.is_empty(), "{name} is not listed");
    }
}

fn owners(workspace: &management::OrganizationWorkspace) -> Vec<(&str, bool)> {
    let mut owners: Vec<_> = workspace
        .owners
        .iter()
        .map(|owner| {
            assert_eq!(owner.profile.as_ref().unwrap().user, owner.user);
            (owner.user.as_str(), owner.enabled)
        })
        .collect();
    owners.sort_unstable();
    owners
}

fn failure(status: &tonic::Status) -> (tonic::Code, String) {
    (status.code(), error_reason(status))
}

/// coral-workspace owns the governance rules (`governance` tests). This checks
/// that each RPC reaches them without a Membership and projects the Workspace.
#[tokio::test]
async fn organization_owner_lists_archives_restores_and_assigns_without_a_membership() {
    let f = Fixture::new().await;
    let mut client = f.organization_workspaces();
    let token = f.token(&f.owner, "reef");
    let name = members_workspace(&f).await;
    let concealed = f
        .workspace_service()
        .get_workspace(request(
            management::GetWorkspaceRequest { name: name.clone() },
            &token,
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(
        failure(&concealed),
        (tonic::Code::NotFound, "NOT_FOUND".into())
    );

    let found = listed(&mut client, &token, &name).await;
    assert_eq!(found.display_name, "Members only");
    assert_eq!(found.state, management::WorkspaceState::Active as i32);
    assert!(!found.etag.is_empty());
    assert_eq!(owners(&found), [("users/member", true)]);
    let own = format!("workspaces/{}", f.workspace.id());
    assert_eq!(
        owners(&listed(&mut client, &token, &own).await),
        [("users/owner", true)]
    );

    let archived = client
        .archive_organization_workspace(request(
            management::ArchiveOrganizationWorkspaceRequest {
                name: name.clone(),
                etag: found.etag.clone(),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(archived.state, management::WorkspaceState::Archived as i32);
    assert_ne!(archived.etag, found.etag);
    assert_eq!(
        management::OrganizationWorkspace {
            state: found.state,
            etag: found.etag.clone(),
            ..archived.clone()
        },
        found,
        "archive changes only the state and etag"
    );
    let restored = client
        .restore_organization_workspace(request(
            management::RestoreOrganizationWorkspaceRequest {
                name: name.clone(),
                etag: archived.etag.clone(),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(restored.state, management::WorkspaceState::Active as i32);
    assert_ne!(restored.etag, archived.etag);

    let assigned = client
        .assign_workspace_owner(request(
            management::AssignWorkspaceOwnerRequest {
                name: name.clone(),
                owner: "organizationMemberships/owner".into(),
                request_id: uuid::Uuid::new_v4().to_string(),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(assigned.name, name);
    assert_eq!(assigned.display_name, "Members only");
    assert_eq!(
        owners(&assigned),
        [("users/member", true), ("users/owner", true)]
    );
    assert_eq!(listed(&mut client, &token, &name).await, assigned);
}

/// Organization Owner authority gates the whole service. An Organization
/// Member is refused even in a Workspace they own, and nothing changes.
#[tokio::test]
async fn organization_workspace_service_refuses_an_organization_member() {
    let f = Fixture::new().await;
    let mut client = f.organization_workspaces();
    let owner = f.token(&f.owner, "reef");
    let member = f.token(&f.member, "reef");
    let name = members_workspace(&f).await;
    let before = listed(&mut client, &owner, &name).await;
    let archived_name = {
        let workspace = f
            .workspaces
            .create(
                &change(&f.member),
                WorkspaceDisplayName::parse("Archived").unwrap(),
            )
            .await
            .unwrap();
        format!("workspaces/{}", workspace.id())
    };
    let archived = listed(&mut client, &owner, &archived_name).await;
    let archived = client
        .archive_organization_workspace(request(
            management::ArchiveOrganizationWorkspaceRequest {
                name: archived_name.clone(),
                etag: archived.etag,
            },
            &owner,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();

    let denials = [
        (
            "list",
            client
                .list_organization_workspaces(request(
                    management::ListOrganizationWorkspacesRequest::default(),
                    &member,
                    "reef",
                ))
                .await
                .unwrap_err(),
        ),
        (
            "assign",
            client
                .assign_workspace_owner(request(
                    management::AssignWorkspaceOwnerRequest {
                        name: name.clone(),
                        owner: "organizationMemberships/owner".into(),
                        request_id: uuid::Uuid::new_v4().to_string(),
                    },
                    &member,
                    "reef",
                ))
                .await
                .unwrap_err(),
        ),
        (
            "archive",
            client
                .archive_organization_workspace(request(
                    management::ArchiveOrganizationWorkspaceRequest {
                        name: name.clone(),
                        etag: before.etag.clone(),
                    },
                    &member,
                    "reef",
                ))
                .await
                .unwrap_err(),
        ),
        (
            "restore",
            client
                .restore_organization_workspace(request(
                    management::RestoreOrganizationWorkspaceRequest {
                        name: archived_name.clone(),
                        etag: archived.etag.clone(),
                    },
                    &member,
                    "reef",
                ))
                .await
                .unwrap_err(),
        ),
    ];
    for (rpc, error) in &denials {
        assert_eq!(
            failure(error),
            (tonic::Code::PermissionDenied, "PERMISSION_DENIED".into()),
            "{rpc}"
        );
    }
    assert_eq!(listed(&mut client, &owner, &name).await, before);
    assert_eq!(listed(&mut client, &owner, &archived_name).await, archived);
}

/// coral-workspace and coral-administration own the Owner guards. This checks
/// that each reaches the wire as its own `FAILED_PRECONDITION` reason.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table reaches three guards through three services and checks each changed nothing"
)]
async fn owner_guards_reach_the_wire_as_distinct_preconditions() {
    let f = Fixture::new().await;
    let mut client = f.organization_workspaces();
    let token = f.token(&f.owner, "reef");
    let name = members_workspace(&f).await;
    let disabled = f
        .organization_user("disabled", OrganizationRole::Member)
        .await;
    OrganizationModule::new(f.database.database())
        .set_enabled(&change(&f.owner), disabled.user_id(), 1, false)
        .await
        .unwrap();
    let sole_owner = f
        .memberships()
        .get_workspace_membership(request(
            management::GetWorkspaceMembershipRequest {
                name: format!("workspaces/{}/memberships/owner", f.workspace.id()),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let organization_memberships = async || {
        f.organization_memberships()
            .list_organization_memberships(request(
                management::ListOrganizationMembershipsRequest::default(),
                &token,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner()
            .memberships
    };
    let members_before = organization_memberships().await;
    let member = members_before
        .iter()
        .find(|m| m.name == "organizationMemberships/member")
        .unwrap();
    let before = listed(&mut client, &token, &name).await;

    let denials = [
        (
            "LAST_WORKSPACE_OWNER",
            f.memberships()
                .delete_workspace_membership(request(
                    management::DeleteWorkspaceMembershipRequest {
                        name: sole_owner.name.clone(),
                        etag: sole_owner.etag.clone(),
                    },
                    &token,
                    "reef",
                ))
                .await
                .unwrap_err(),
        ),
        (
            "OWNER_TARGET_NOT_ENABLED",
            client
                .assign_workspace_owner(request(
                    management::AssignWorkspaceOwnerRequest {
                        name: name.clone(),
                        owner: "organizationMemberships/disabled".into(),
                        request_id: uuid::Uuid::new_v4().to_string(),
                    },
                    &token,
                    "reef",
                ))
                .await
                .unwrap_err(),
        ),
        (
            "OWNER_TRANSFER_REQUIRED",
            f.organization_memberships()
                .delete_organization_membership(request(
                    management::DeleteOrganizationMembershipRequest {
                        name: member.name.clone(),
                        etag: member.etag.clone(),
                        transfer_to: String::new(),
                    },
                    &token,
                    "reef",
                ))
                .await
                .unwrap_err(),
        ),
    ];
    for (reason, error) in &denials {
        assert_eq!(
            failure(error),
            (tonic::Code::FailedPrecondition, (*reason).into())
        );
    }
    assert_eq!(
        f.memberships()
            .get_workspace_membership(request(
                management::GetWorkspaceMembershipRequest {
                    name: sole_owner.name.clone(),
                },
                &token,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner(),
        sole_owner
    );
    assert_eq!(listed(&mut client, &token, &name).await, before);
    assert_eq!(organization_memberships().await, members_before);
}

/// coral-administration owns the transfer
/// (`organization_removal_of_a_sole_workspace_owner_transfers_ownership_or_changes_nothing`). This checks that
/// `transfer_to` names an Organization Membership and reaches the removal.
#[tokio::test]
async fn organization_membership_removal_carries_the_transfer_target() {
    let f = Fixture::new().await;
    let mut client = f.organization_workspaces();
    let token = f.token(&f.owner, "reef");
    let name = members_workspace(&f).await;
    let mut memberships = f.organization_memberships();
    let member = memberships
        .list_organization_memberships(request(
            management::ListOrganizationMembershipsRequest::default(),
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner()
        .memberships
        .into_iter()
        .find(|m| m.name == "organizationMemberships/member")
        .unwrap();
    let before = listed(&mut client, &token, &name).await;
    let remove = |transfer_to: &str| {
        request(
            management::DeleteOrganizationMembershipRequest {
                name: member.name.clone(),
                etag: member.etag.clone(),
                transfer_to: transfer_to.into(),
            },
            &token,
            "reef",
        )
    };
    for malformed in ["users/owner", "owner", "organizationMemberships/owner/x"] {
        let error = memberships
            .delete_organization_membership(remove(malformed))
            .await
            .unwrap_err();
        assert_eq!(
            failure(&error),
            (tonic::Code::InvalidArgument, "INVALID_ARGUMENT".into()),
            "{malformed}"
        );
    }
    assert_eq!(listed(&mut client, &token, &name).await, before);

    memberships
        .delete_organization_membership(remove("organizationMemberships/owner"))
        .await
        .unwrap();
    let transferred = listed(&mut client, &token, &name).await;
    assert_eq!(owners(&transferred), [("users/owner", true)]);
    assert!(
        memberships
            .list_organization_memberships(request(
                management::ListOrganizationMembershipsRequest::default(),
                &token,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner()
            .memberships
            .iter()
            .all(|m| m.name != member.name)
    );
}
